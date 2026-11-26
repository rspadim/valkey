/*
 * Copyright (c) 2009-2012, Redis Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "cluster.h"
#include "cluster_slot_stats.h"

/* Structure to hold the pubsub related metadata. Currently used
 * for pubsub and pubsubshard feature. */
typedef struct pubsubtype {
    int shard;
    hashtable *(*clientPubSubChannels)(client *);
    int (*subscriptionCount)(client *);
    kvstore **serverPubSubChannels;
    robj **subscribeMsg;
    robj **unsubscribeMsg;
    robj **messageBulk;
} pubsubtype;

/*
 * Get client's global Pub/Sub channels subscription count.
 */
int clientSubscriptionsCount(client *c);

/*
 * Get client's shard level Pub/Sub channels subscription count.
 */
int clientShardSubscriptionsCount(client *c);

/*
 * Get client's global Pub/Sub channels dict.
 */
hashtable *getClientPubSubChannels(client *c);

/*
 * Get client's shard level Pub/Sub channels dict.
 */
hashtable *getClientPubSubShardChannels(client *c);

/*
 * Get list of channels client is subscribed to.
 * If a pattern is provided, the subset of channels is returned
 * matching the pattern.
 */
void channelList(client *c, sds pat, kvstore *pubsub_channels);

/* Conflated Pub/Sub helpers defined later in this file. */
int pubsubUnsubscribeAllConflatedChannels(client *c, int notify);
int pubsubUnsubscribeAllConflatedPatterns(client *c, int notify);
int pubsubUnsubscribeAllConflatedShardChannels(client *c, int notify);
int pubsubPublishConflatedChannel(robj *channel, robj *message);
int pubsubPublishConflatedPatterns(robj *channel, robj *message);
int pubsubPublishConflatedShardedChannel(robj *channel, robj *message, int slot);

/*
 * Pub/Sub type for global channels.
 */
pubsubtype pubSubType = {
    .shard = 0,
    .clientPubSubChannels = getClientPubSubChannels,
    .subscriptionCount = clientSubscriptionsCount,
    .serverPubSubChannels = &server.pubsub_channels,
    .subscribeMsg = &shared.subscribebulk,
    .unsubscribeMsg = &shared.unsubscribebulk,
    .messageBulk = &shared.messagebulk,
};

/* State of one CSUBSCRIBE subscription. `payload` is the latest value not yet
 * delivered; it is a refcounted shared object, so replacing it costs one
 * incref/decref and no byte copy. */
typedef struct conflatedSub {
    long long interval_ms; /* Delivery interval in milliseconds. */
    mstime_t due;          /* When the pending payload is due; 0 = nothing pending. */
    robj *payload;         /* Latest payload not yet delivered, or NULL. */
} conflatedSub;

static void conflatedSubDestructor(void *value) {
    conflatedSub *sub = value;
    if (sub->payload) decrRefCount(sub->payload);
    zfree(sub);
}

/* Object key -> conflatedSub value. */
static void conflatedSubDictEntryDestructor(void *entry) {
    dictEntry *de = entry;
    conflatedSubDestructor(dictGetVal(de));
    decrRefCount(dictGetKey(de));
    zfree(de);
}

static dictType conflatedSubDictType = {
    .entryGetKey = dictEntryGetKey,
    .hashFunction = dictObjHash,
    .keyCompare = dictObjKeyCompare,
    .entryDestructor = conflatedSubDictEntryDestructor,
};

/* One CPSUBSCRIBE pattern: its delivery interval plus the per-concrete-channel
 * pending slots. The interval is captured at subscribe time and reused for
 * every new channel matched later. */
typedef struct conflatedPatternSub {
    long long interval_ms;
    dict *channels;
} conflatedPatternSub;

static void conflatedPatternSubDestructor(void *value) {
    conflatedPatternSub *ps = value;
    dictRelease(ps->channels);
    zfree(ps);
}

static void conflatedPatternSubDictEntryDestructor(void *entry) {
    dictEntry *de = entry;
    conflatedPatternSubDestructor(dictGetVal(de));
    decrRefCount(dictGetKey(de));
    zfree(de);
}

static dictType conflatedPatternSubDictType = {
    .entryGetKey = dictEntryGetKey,
    .hashFunction = dictObjHash,
    .keyCompare = dictObjKeyCompare,
    .entryDestructor = conflatedPatternSubDictEntryDestructor,
};

/* Pending accounting. These gauges are the only memory bound for conflated
 * delivery, so every path that stores, replaces, delivers or evicts a payload
 * must go through them. */
static size_t conflatedPayloadBytes(robj *payload) {
    if (payload == NULL) return 0;
    if (payload->encoding == OBJ_ENCODING_INT) return sizeof(long);
    return sdslen(objectGetVal(payload));
}

static void conflatedAccountPayload(client *c, size_t bytes, int add) {
    if (add) {
        c->pubsub_data->conflated_pending_bytes += bytes;
        c->pubsub_data->conflated_pending_slots++;
        server.pubsub_conflated_pending++;
        server.pubsub_conflated_pending_bytes += bytes;
        return;
    }
    serverAssert(c->pubsub_data->conflated_pending_bytes >= bytes);
    serverAssert(c->pubsub_data->conflated_pending_slots > 0);
    serverAssert(server.pubsub_conflated_pending > 0);
    serverAssert(server.pubsub_conflated_pending_bytes >= bytes);
    c->pubsub_data->conflated_pending_bytes -= bytes;
    c->pubsub_data->conflated_pending_slots--;
    server.pubsub_conflated_pending--;
    server.pubsub_conflated_pending_bytes -= bytes;
}

static void conflatedAccountPatternEntry(client *c, int add) {
    if (add) {
        c->pubsub_data->conflated_pattern_entries++;
        server.pubsub_conflated_pattern_entries++;
        return;
    }
    serverAssert(c->pubsub_data->conflated_pattern_entries > 0);
    serverAssert(server.pubsub_conflated_pattern_entries > 0);
    c->pubsub_data->conflated_pattern_entries--;
    server.pubsub_conflated_pattern_entries--;
}

static void conflatedClearPayload(client *c, conflatedSub *sub) {
    if (sub->payload == NULL) return;
    size_t bytes = conflatedPayloadBytes(sub->payload);
    decrRefCount(sub->payload);
    sub->payload = NULL;
    sub->due = 0;
    conflatedAccountPayload(c, bytes, 0);
    /* Force the next timer tick to recompute the earliest due time. */
    server.pubsub_conflated_next_due = 0;
}

/* Record a newly pending due time so an idle timer can skip the full scan. */
static void conflatedMarkDue(mstime_t due) {
    /* 0 means "unknown, force a scan": do not hide an earlier pending due
     * that was cleared before this publish. */
    if (server.pubsub_conflated_next_due != 0 &&
        (server.pubsub_conflated_next_due == LLONG_MAX || due < server.pubsub_conflated_next_due)) {
        server.pubsub_conflated_next_due = due;
    }
}

/* Recompute the earliest pending due after a delivery/clear pass. */
static void conflatedRecomputeNextDue(void) {
    mstime_t next = LLONG_MAX;
    dictIterator *di = dictGetIterator(server.pubsub_conflated_channels);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        hashtable *clients = dictGetVal(de);
        hashtableIterator iter;
        hashtableInitIterator(&iter, clients, 0);
        void *entry;
        while (hashtableNext(&iter, &entry)) {
            client *c = entry;
            if (!c->pubsub_data) continue;
            conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_channels, dictGetKey(de));
            if (sub && sub->payload && sub->due < next) next = sub->due;
        }
        hashtableCleanupIterator(&iter);
    }
    dictReleaseIterator(di);

    dictIterator *pdi = dictGetIterator(server.pubsub_conflated_patterns);
    dictEntry *pde;
    while ((pde = dictNext(pdi)) != NULL) {
        hashtable *clients = dictGetVal(pde);
        hashtableIterator iter;
        hashtableInitIterator(&iter, clients, 0);
        void *entry;
        while (hashtableNext(&iter, &entry)) {
            client *c = entry;
            if (!c->pubsub_data) continue;
            conflatedPatternSub *ps = dictFetchValue(c->pubsub_data->conflated_patterns, dictGetKey(pde));
            if (!ps) continue;
            dictIterator *cdi = dictGetIterator(ps->channels);
            dictEntry *cde;
            while ((cde = dictNext(cdi)) != NULL) {
                conflatedSub *sub = dictGetVal(cde);
                if (sub->payload && sub->due < next) next = sub->due;
            }
            dictReleaseIterator(cdi);
        }
        hashtableCleanupIterator(&iter);
    }
    dictReleaseIterator(pdi);

    kvstoreIterator *kvs_it = kvstoreIteratorInit(server.pubsub_conflated_shard_channels, 0);
    void *element;
    while (kvstoreIteratorNext(kvs_it, &element)) {
        hashtable *clients = element;
        robj *channel = *(robj **)hashtableMetadata(clients);
        hashtableIterator iter;
        hashtableInitIterator(&iter, clients, 0);
        void *entry;
        while (hashtableNext(&iter, &entry)) {
            client *c = entry;
            if (!c->pubsub_data) continue;
            conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_shard_channels, channel);
            if (sub && sub->payload && sub->due < next) next = sub->due;
        }
        hashtableCleanupIterator(&iter);
    }
    kvstoreIteratorRelease(kvs_it);
    server.pubsub_conflated_next_due = next;
}

/* Re-schedule all pending values after conflated-pubsub-interval-ms changes. */
void pubsubConflatedReschedule(void) {
    if (!server.pubsub_conflated_channels || !server.pubsub_conflated_patterns ||
        !server.pubsub_conflated_shard_channels)
        return;
    long long interval = server.pubsub_conflated_interval_ms;
    if (interval <= 0) return;
    mstime_t due = mstime() + interval;

    dictIterator *di = dictGetIterator(server.pubsub_conflated_channels);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        hashtable *clients = dictGetVal(de);
        hashtableIterator iter;
        hashtableInitIterator(&iter, clients, 0);
        void *entry;
        while (hashtableNext(&iter, &entry)) {
            client *c = entry;
            if (!c->pubsub_data) continue;
            conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_channels, dictGetKey(de));
            if (sub && sub->payload) sub->due = due;
        }
        hashtableCleanupIterator(&iter);
    }
    dictReleaseIterator(di);

    dictIterator *pdi = dictGetIterator(server.pubsub_conflated_patterns);
    dictEntry *pde;
    while ((pde = dictNext(pdi)) != NULL) {
        hashtable *clients = dictGetVal(pde);
        hashtableIterator iter;
        hashtableInitIterator(&iter, clients, 0);
        void *entry;
        while (hashtableNext(&iter, &entry)) {
            client *c = entry;
            if (!c->pubsub_data) continue;
            conflatedPatternSub *ps = dictFetchValue(c->pubsub_data->conflated_patterns, dictGetKey(pde));
            if (!ps) continue;
            dictIterator *cdi = dictGetIterator(ps->channels);
            dictEntry *cde;
            while ((cde = dictNext(cdi)) != NULL) {
                conflatedSub *sub = dictGetVal(cde);
                if (sub->payload) sub->due = due;
            }
            dictReleaseIterator(cdi);
        }
        hashtableCleanupIterator(&iter);
    }
    dictReleaseIterator(pdi);

    kvstoreIterator *kvs_it = kvstoreIteratorInit(server.pubsub_conflated_shard_channels, 0);
    void *element;
    while (kvstoreIteratorNext(kvs_it, &element)) {
        hashtable *clients = element;
        robj *channel = *(robj **)hashtableMetadata(clients);
        hashtableIterator iter;
        hashtableInitIterator(&iter, clients, 0);
        void *entry;
        while (hashtableNext(&iter, &entry)) {
            client *c = entry;
            if (!c->pubsub_data) continue;
            conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_shard_channels, channel);
            if (sub && sub->payload) sub->due = due;
        }
        hashtableCleanupIterator(&iter);
    }
    kvstoreIteratorRelease(kvs_it);
    conflatedRecomputeNextDue();
}

/* Recycle a pattern channel entry that was already delivered. It costs no
 * payload bytes and is the cheapest way to make room for a new channel. */
static int conflatedEvictNullPatternEntry(client *c) {
    dictIterator *pdi = dictGetIterator(c->pubsub_data->conflated_patterns);
    dictEntry *pde;
    while ((pde = dictNext(pdi)) != NULL) {
        conflatedPatternSub *ps = dictGetVal(pde);
        dict *channels = ps->channels;
        dictIterator *cdi = dictGetSafeIterator(channels);
        dictEntry *cde;
        while ((cde = dictNext(cdi)) != NULL) {
            conflatedSub *sub = dictGetVal(cde);
            if (sub->payload != NULL) continue;
            dictDelete(channels, dictGetKey(cde));
            conflatedAccountPatternEntry(c, 0);
            server.pubsub_conflated_evicted++;
            dictReleaseIterator(cdi);
            dictReleaseIterator(pdi);
            return 1;
        }
        dictReleaseIterator(cdi);
    }
    dictReleaseIterator(pdi);
    return 0;
}

/* Evict the pending value with the oldest due time. Exact subscriptions stay
 * (only their pending value is cleared); pattern channel entries are removed
 * entirely. `protect` is the entry being updated by the caller and must not be
 * freed under it: when it is the victim, only its payload is cleared. */
static int conflatedEvictOldest(client *c, int protect_pattern, conflatedSub *protect) {
    mstime_t oldest = LLONG_MAX;
    int kind = 0; /* 0 none, 1 exact, 2 pattern, 3 shard */
    conflatedSub *victim = NULL;
    robj *victim_channel = NULL;
    dict *victim_channels = NULL;

    dictIterator *di = dictGetIterator(c->pubsub_data->conflated_channels);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        conflatedSub *sub = dictGetVal(de);
        if (sub->payload != NULL && sub->due < oldest) {
            oldest = sub->due;
            kind = 1;
            victim = sub;
        }
    }
    dictReleaseIterator(di);

    dictIterator *pdi = dictGetIterator(c->pubsub_data->conflated_patterns);
    dictEntry *pde;
    while ((pde = dictNext(pdi)) != NULL) {
        conflatedPatternSub *ps = dictGetVal(pde);
        dict *channels = ps->channels;
        dictIterator *cdi = dictGetIterator(channels);
        dictEntry *cde;
        while ((cde = dictNext(cdi)) != NULL) {
            conflatedSub *sub = dictGetVal(cde);
            if (sub->payload != NULL && sub->due < oldest) {
                oldest = sub->due;
                kind = 2;
                victim = sub;
                victim_channel = dictGetKey(cde);
                victim_channels = channels;
            }
        }
        dictReleaseIterator(cdi);
    }
    dictReleaseIterator(pdi);

    dictIterator *sdi = dictGetIterator(c->pubsub_data->conflated_shard_channels);
    dictEntry *sde;
    while ((sde = dictNext(sdi)) != NULL) {
        conflatedSub *sub = dictGetVal(sde);
        if (sub->payload != NULL && sub->due < oldest) {
            oldest = sub->due;
            kind = 3;
            victim = sub;
        }
    }
    dictReleaseIterator(sdi);

    if (kind == 0) return 0;
    conflatedClearPayload(c, victim);
    if (kind == 2 && !(protect_pattern && victim == protect)) {
        dictDelete(victim_channels, victim_channel);
        conflatedAccountPatternEntry(c, 0);
    }
    server.pubsub_conflated_evicted++;
    return 1;
}

static int conflatedCapsExceeded(client *c, size_t delta, int new_slot, int new_entry) {
    size_t client_cap = (size_t)server.pubsub_conflated_max_pending_bytes;
    size_t total_cap = (size_t)server.pubsub_conflated_max_total_pending_bytes;
    size_t client_bytes = c->pubsub_data->conflated_pending_bytes;
    size_t total_bytes = server.pubsub_conflated_pending_bytes;

    if (client_cap != 0 && (client_bytes > client_cap || delta > client_cap - client_bytes)) return 1;
    if (total_cap != 0 && (total_bytes > total_cap || delta > total_cap - total_bytes)) return 1;

    unsigned long long key_cap = (unsigned long long)server.pubsub_conflated_max_pending_keys;
    if (key_cap != 0 && c->pubsub_data->conflated_pattern_entries + (unsigned)new_entry > key_cap) return 1;

    unsigned long long total_key_cap = (unsigned long long)server.pubsub_conflated_max_total_pending_keys;
    if (total_key_cap != 0 &&
        server.pubsub_conflated_pending + server.pubsub_conflated_pattern_entries + (unsigned)(new_slot + new_entry) >
            total_key_cap)
        return 1;
    return 0;
}

/* Reserve capacity before storing a value. `delta` is the net payload byte
 * growth and `protect` is the pattern entry being updated (never freed here). */
static int conflatedReserve(client *c, size_t delta, int new_slot, int new_entry, int protect_pattern,
                            conflatedSub *protect) {
    for (int attempt = 0; attempt < 16; attempt++) {
        if (!conflatedCapsExceeded(c, delta, new_slot, new_entry)) return 1;
        if (server.pubsub_conflated_overflow_policy == PUBSUB_CONFLATED_DROP_NEWEST) {
            if (new_entry && conflatedEvictNullPatternEntry(c)) continue;
            return 0;
        }
        if (new_entry && conflatedEvictNullPatternEntry(c)) continue;
        if (!conflatedEvictOldest(c, protect_pattern, protect)) return 0;
    }
    return 0;
}

/*
 * Pub/Sub type for shard level channels bounded to a slot.
 */
pubsubtype pubSubShardType = {
    .shard = 1,
    .clientPubSubChannels = getClientPubSubShardChannels,
    .subscriptionCount = clientShardSubscriptionsCount,
    .serverPubSubChannels = &server.pubsubshard_channels,
    .subscribeMsg = &shared.ssubscribebulk,
    .unsubscribeMsg = &shared.sunsubscribebulk,
    .messageBulk = &shared.smessagebulk,
};

/*-----------------------------------------------------------------------------
 * Pubsub client replies API
 *----------------------------------------------------------------------------*/

/* Send a pubsub message of type "message" to the client.
 * Normally 'msg' is an Object containing the string to send as
 * message. However if the caller sets 'msg' as NULL, it will be able
 * to send a special message (for instance an Array type) by using the
 * addReply*() API family. */
void addReplyPubsubMessage(client *c, robj *channel, robj *msg, robj *message_bulk) {
    struct ClientFlags old_flags = c->flag;
    c->flag.pushing = 1;
    if (c->resp == 2)
        addReply(c, shared.mbulkhdr[3]);
    else
        addReplyPushLen(c, 3);
    addReply(c, message_bulk);
    addReplyBulk(c, channel);
    if (msg) addReplyBulk(c, msg);
    if (!old_flags.pushing) c->flag.pushing = 0;
}

/* Send a pubsub message of type "pmessage" to the client. The difference
 * with the "message" type delivered by addReplyPubsubMessage() is that
 * this message format also includes the pattern that matched the message. */
void addReplyPubsubPatMessage(client *c, robj *pat, robj *channel, robj *msg) {
    struct ClientFlags old_flags = c->flag;
    c->flag.pushing = 1;
    if (c->resp == 2)
        addReply(c, shared.mbulkhdr[4]);
    else
        addReplyPushLen(c, 4);
    addReply(c, shared.pmessagebulk);
    addReplyBulk(c, pat);
    addReplyBulk(c, channel);
    addReplyBulk(c, msg);
    if (!old_flags.pushing) c->flag.pushing = 0;
}

/* Send the pubsub subscription notification to the client. */
void addReplyPubsubSubscribed(client *c, robj *channel, pubsubtype type) {
    struct ClientFlags old_flags = c->flag;
    c->flag.pushing = 1;
    if (c->resp == 2)
        addReply(c, shared.mbulkhdr[3]);
    else
        addReplyPushLen(c, 3);
    addReply(c, *type.subscribeMsg);
    addReplyBulk(c, channel);
    addReplyLongLong(c, type.subscriptionCount(c));
    if (!old_flags.pushing) c->flag.pushing = 0;
}

/* Send the pubsub unsubscription notification to the client.
 * Channel can be NULL: this is useful when the client sends a mass
 * unsubscribe command but there are no channels to unsubscribe from: we
 * still send a notification. */
void addReplyPubsubUnsubscribed(client *c, robj *channel, pubsubtype type) {
    struct ClientFlags old_flags = c->flag;
    c->flag.pushing = 1;
    if (c->resp == 2)
        addReply(c, shared.mbulkhdr[3]);
    else
        addReplyPushLen(c, 3);
    addReply(c, *type.unsubscribeMsg);
    if (channel)
        addReplyBulk(c, channel);
    else
        addReplyNull(c);
    addReplyLongLong(c, type.subscriptionCount(c));
    if (!old_flags.pushing) c->flag.pushing = 0;
}

/* Send the pubsub pattern subscription notification to the client. */
void addReplyPubsubPatSubscribed(client *c, robj *pattern) {
    struct ClientFlags old_flags = c->flag;
    c->flag.pushing = 1;
    if (c->resp == 2)
        addReply(c, shared.mbulkhdr[3]);
    else
        addReplyPushLen(c, 3);
    addReply(c, shared.psubscribebulk);
    addReplyBulk(c, pattern);
    addReplyLongLong(c, clientSubscriptionsCount(c));
    if (!old_flags.pushing) c->flag.pushing = 0;
}

/* Send the pubsub pattern unsubscription notification to the client.
 * Pattern can be NULL: this is useful when the client sends a mass
 * punsubscribe command but there are no pattern to unsubscribe from: we
 * still send a notification. */
void addReplyPubsubPatUnsubscribed(client *c, robj *pattern) {
    struct ClientFlags old_flags = c->flag;
    c->flag.pushing = 1;
    if (c->resp == 2)
        addReply(c, shared.mbulkhdr[3]);
    else
        addReplyPushLen(c, 3);
    addReply(c, shared.punsubscribebulk);
    if (pattern)
        addReplyBulk(c, pattern);
    else
        addReplyNull(c);
    addReplyLongLong(c, clientSubscriptionsCount(c));
    if (!old_flags.pushing) c->flag.pushing = 0;
}

/*-----------------------------------------------------------------------------
 * Pubsub low level API
 *----------------------------------------------------------------------------*/

/* Return the number of pubsub channels + patterns is handled. */
int serverPubsubSubscriptionCount(void) {
    return kvstoreSize(server.pubsub_channels) + dictSize(server.pubsub_patterns) +
           dictSize(server.pubsub_conflated_channels) + dictSize(server.pubsub_conflated_patterns);
}

/* Return the number of pubsub shard level channels is handled. */
int serverPubsubShardSubscriptionCount(void) {
    return kvstoreSize(server.pubsubshard_channels) + kvstoreSize(server.pubsub_conflated_shard_channels);
}

/* Return the number of channels + patterns a client is subscribed to. */
int clientSubscriptionsCount(client *c) {
    return hashtableSize(c->pubsub_data->pubsub_channels) + hashtableSize(c->pubsub_data->pubsub_patterns) +
           hashtableSize(c->pubsub_data->conflated_channels) + hashtableSize(c->pubsub_data->conflated_patterns);
}

/* Return the number of shard level channels a client is subscribed to. */
int clientShardSubscriptionsCount(client *c) {
    return hashtableSize(c->pubsub_data->pubsubshard_channels) +
           hashtableSize(c->pubsub_data->conflated_shard_channels);
}

hashtable *getClientPubSubChannels(client *c) {
    return c->pubsub_data->pubsub_channels;
}

hashtable *getClientPubSubShardChannels(client *c) {
    return c->pubsub_data->pubsubshard_channels;
}

/* Defrag the conflated server-side tables and the SDS data of shared channel
 * names. The robj headers stay in place, so every index keeps its pointers. */
void pubsubDefragConflatedServer(void) {
    hashtable *new_ht = hashtableDefragTables(server.pubsub_conflated_channels, activeDefragAlloc);
    if (new_ht) server.pubsub_conflated_channels = new_ht;
    new_ht = hashtableDefragTables(server.pubsub_conflated_patterns, activeDefragAlloc);
    if (new_ht) server.pubsub_conflated_patterns = new_ht;

    dictIterator *di = dictGetIterator(server.pubsub_conflated_channels);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        defragStringObjectSds(dictGetKey(de));
        hashtable *clients = dictGetVal(de);
        new_ht = hashtableDefragTables(clients, activeDefragAlloc);
        if (new_ht) dictSetVal(server.pubsub_conflated_channels, de, new_ht);
    }
    dictReleaseIterator(di);

    di = dictGetIterator(server.pubsub_conflated_patterns);
    while ((de = dictNext(di)) != NULL) {
        defragStringObjectSds(dictGetKey(de));
        hashtable *clients = dictGetVal(de);
        new_ht = hashtableDefragTables(clients, activeDefragAlloc);
        if (new_ht) dictSetVal(server.pubsub_conflated_patterns, de, new_ht);
    }
    dictReleaseIterator(di);
}

/* Defrag one client's conflated tables, including nested pattern channel
 * tables. */
void pubsubDefragConflatedClient(client *c) {
    if (!c->pubsub_data) return;
    server.stat_active_defrag_scanned++;
    hashtable *new_ht = hashtableDefragTables(c->pubsub_data->conflated_channels, activeDefragAlloc);
    if (new_ht) c->pubsub_data->conflated_channels = new_ht;
    new_ht = hashtableDefragTables(c->pubsub_data->conflated_patterns, activeDefragAlloc);
    if (new_ht) c->pubsub_data->conflated_patterns = new_ht;
    new_ht = hashtableDefragTables(c->pubsub_data->conflated_shard_channels, activeDefragAlloc);
    if (new_ht) c->pubsub_data->conflated_shard_channels = new_ht;

    dictIterator *cdi = dictGetIterator(c->pubsub_data->conflated_channels);
    dictEntry *cde;
    while ((cde = dictNext(cdi)) != NULL) {
        defragStringObjectSds(dictGetKey(cde));
        conflatedSub *sub = dictGetVal(cde);
        defragStringObjectSds(sub->payload);
        conflatedSub *new_sub = activeDefragAlloc(sub);
        if (new_sub) dictSetVal(c->pubsub_data->conflated_channels, cde, new_sub);
    }
    dictReleaseIterator(cdi);

    cdi = dictGetIterator(c->pubsub_data->conflated_shard_channels);
    while ((cde = dictNext(cdi)) != NULL) {
        defragStringObjectSds(dictGetKey(cde));
        conflatedSub *sub = dictGetVal(cde);
        defragStringObjectSds(sub->payload);
        conflatedSub *new_sub = activeDefragAlloc(sub);
        if (new_sub) dictSetVal(c->pubsub_data->conflated_shard_channels, cde, new_sub);
    }
    dictReleaseIterator(cdi);

    dictIterator *di = dictGetIterator(c->pubsub_data->conflated_patterns);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        defragStringObjectSds(dictGetKey(de));
        conflatedPatternSub *ps = dictGetVal(de);
        conflatedPatternSub *new_ps = activeDefragAlloc(ps);
        if (new_ps) {
            dictSetVal(c->pubsub_data->conflated_patterns, de, new_ps);
            ps = new_ps;
        }
        new_ht = hashtableDefragTables(ps->channels, activeDefragAlloc);
        if (new_ht) ps->channels = new_ht;

        dictIterator *pdi = dictGetIterator(ps->channels);
        dictEntry *pde;
        while ((pde = dictNext(pdi)) != NULL) {
            defragStringObjectSds(dictGetKey(pde));
            conflatedSub *sub = dictGetVal(pde);
            defragStringObjectSds(sub->payload);
            conflatedSub *new_sub = activeDefragAlloc(sub);
            if (new_sub) dictSetVal(ps->channels, pde, new_sub);
        }
        dictReleaseIterator(pdi);
    }
    dictReleaseIterator(di);
}

/* Return the number of pubsub + pubsub shard level channels
 * a client is subscribed to. */
int clientTotalPubSubSubscriptionCount(client *c) {
    return clientSubscriptionsCount(c) + clientShardSubscriptionsCount(c);
}

void markClientAsPubSub(client *c) {
    if (!c->flag.pubsub) {
        c->flag.pubsub = 1;
        server.pubsub_clients++;
    }
}

void unmarkClientAsPubSub(client *c) {
    if (c->flag.pubsub) {
        c->flag.pubsub = 0;
        server.pubsub_clients--;
    }
}

void initClientPubSubData(client *c) {
    if (c->pubsub_data) return;
    c->pubsub_data = zmalloc(sizeof(ClientPubSubData));
    c->pubsub_data->pubsub_channels = hashtableCreate(&objectHashtableType);
    c->pubsub_data->pubsub_patterns = hashtableCreate(&objectHashtableType);
    c->pubsub_data->pubsubshard_channels = hashtableCreate(&objectHashtableType);
    c->pubsub_data->conflated_channels = hashtableCreate(&conflatedSubDictType);
    c->pubsub_data->conflated_patterns = dictCreate(&conflatedPatternSubDictType);
    c->pubsub_data->conflated_shard_channels = hashtableCreate(&conflatedSubDictType);
    c->pubsub_data->conflated_pending_bytes = 0;
    c->pubsub_data->conflated_pending_slots = 0;
    c->pubsub_data->conflated_pattern_entries = 0;
    c->pubsub_data->pubsub_object_mem = 0;
    c->pubsub_data->client_tracking_redirection = 0;
    c->pubsub_data->client_tracking_prefixes = NULL;
}

void freeClientPubSubData(client *c) {
    if (!c->pubsub_data) return;
    /* Unsubscribe from all the pubsub channels */
    pubsubUnsubscribeAllChannels(c, 0);
    pubsubUnsubscribeShardAllChannels(c, 0);
    pubsubUnsubscribeAllPatterns(c, 0);
    pubsubUnsubscribeAllConflatedChannels(c, 0);
    pubsubUnsubscribeAllConflatedPatterns(c, 0);
    pubsubUnsubscribeAllConflatedShardChannels(c, 0);
    unmarkClientAsPubSub(c);
    hashtableRelease(c->pubsub_data->pubsub_channels);
    c->pubsub_data->pubsub_channels = NULL;
    hashtableRelease(c->pubsub_data->pubsub_patterns);
    c->pubsub_data->pubsub_patterns = NULL;
    hashtableRelease(c->pubsub_data->pubsubshard_channels);
    c->pubsub_data->pubsubshard_channels = NULL;
    hashtableRelease(c->pubsub_data->conflated_channels);
    c->pubsub_data->conflated_channels = NULL;
    hashtableRelease(c->pubsub_data->conflated_patterns);
    c->pubsub_data->conflated_patterns = NULL;
    hashtableRelease(c->pubsub_data->conflated_shard_channels);
    c->pubsub_data->conflated_shard_channels = NULL;
    if (c->pubsub_data->client_tracking_prefixes) {
        disableTracking(c);
    }
    zfree(c->pubsub_data);
    c->pubsub_data = NULL;
}

/* Subscribe a client to a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was already subscribed to that channel. */
int pubsubSubscribeChannel(client *c, robj *channel, pubsubtype type) {
    hashtable *clients = NULL;
    int retval = 0;
    unsigned int slot = 0;

    if (!c->pubsub_data) initClientPubSubData(c);

    /* Add the channel to the client -> channels hash table */
    hashtablePosition position;
    if (hashtableFindPositionForInsert(type.clientPubSubChannels(c), channel, &position, NULL)) {
        /* Not yet subscribed to this channel */
        retval = 1;
        /* Add the client to the channel -> list of clients hash table */
        if (server.cluster_enabled && type.shard) {
            slot = getCachedKeySlot(objectGetVal(channel));
        }

        hashtablePosition pos;
        void *existing;
        if (!kvstoreHashtableFindPositionForInsert(*type.serverPubSubChannels, slot, channel, &pos, &existing)) {
            clients = existing;
            channel = *(robj **)hashtableMetadata(clients);
        } else {
            /* Store pointer to channel name in the dict's metadata. */
            clients = hashtableCreate(&clientHashtableType);
            *(robj **)hashtableMetadata(clients) = channel;
            incrRefCount(channel);
            /* Insert this dict in the kvstore at the position returned above. */
            kvstoreHashtableInsertAtPosition(*type.serverPubSubChannels, slot, clients, &pos);
        }

        serverAssert(hashtableAdd(clients, c));
        hashtableInsertAtPosition(type.clientPubSubChannels(c), channel, &position);
        incrRefCount(channel);
        c->pubsub_data->pubsub_object_mem += getStringObjectMemory(channel);
    }
    /* Notify the client */
    addReplyPubsubSubscribed(c, channel, type);
    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribeChannel(client *c, robj *channel, int notify, pubsubtype type) {
    hashtable *clients;
    int retval = 0;
    int slot = 0;

    /* Remove the channel from the client -> channels hash table */
    incrRefCount(channel); /* channel may be just a pointer to the same object
                            we have in the hash tables. Protect it... */
    void *popped = NULL;
    if (hashtablePop(type.clientPubSubChannels(c), channel, &popped)) {
        retval = 1;
        /* Remove the client from the channel -> clients list hash table */
        if (server.cluster_enabled && type.shard) {
            /* Using keyHashSlot directly because we can't rely on the current_client's slot via getCachedKeySlot() here,
             * as it might differ from the channel's slot. */
            slot = keyHashSlot(objectGetVal(channel), (int)sdslen(objectGetVal(channel)));
        }
        void *found = NULL;
        kvstoreHashtableFind(*type.serverPubSubChannels, slot, channel, &found);
        serverAssertWithInfo(c, NULL, found);
        clients = found;
        serverAssertWithInfo(c, NULL, hashtableDelete(clients, c));
        if (hashtableSize(clients) == 0) {
            /* Free the dict and associated hash entry at all if this was
             * the latest client, so that it will be possible to abuse
             * PUBSUB creating millions of channels. */
            kvstoreHashtableDelete(*type.serverPubSubChannels, slot, channel);
        }
        c->pubsub_data->pubsub_object_mem -= getStringObjectMemory(popped);
        decrRefCount(popped);
    }
    /* Notify the client */
    if (notify) {
        addReplyPubsubUnsubscribed(c, channel, type);
    }
    decrRefCount(channel); /* it is finally safe to release it */
    return retval;
}

/* Unsubscribe all shard channels in a slot. */
void pubsubShardUnsubscribeAllChannelsInSlot(unsigned int slot) {
    if (!kvstoreHashtableSize(server.pubsubshard_channels, slot)) return;

    kvstoreHashtableIterator *kvs_di = kvstoreGetHashtableIterator(server.pubsubshard_channels, slot, HASHTABLE_ITER_SAFE);
    void *element;
    while (kvstoreHashtableIteratorNext(kvs_di, &element)) {
        hashtable *clients = element;
        robj *channel = *(robj **)hashtableMetadata(clients);
        /* For each client subscribed to the channel, unsubscribe it. */
        hashtableIterator client_iter;
        hashtableInitIterator(&client_iter, clients, 0);
        void *next_client;
        while (hashtableNext(&client_iter, &next_client)) {
            client *c = next_client;
            int retval = hashtableDelete(c->pubsub_data->pubsubshard_channels, channel);
            serverAssertWithInfo(c, channel, retval);
            addReplyPubsubUnsubscribed(c, channel, pubSubShardType);
            /* If the client has no other pubsub subscription,
             * move out of pubsub mode. */
            if (clientTotalPubSubSubscriptionCount(c) == 0) {
                unmarkClientAsPubSub(c);
            }
            c->pubsub_data->pubsub_object_mem -= getStringObjectMemory(channel);
        }
        hashtableCleanupIterator(&client_iter);
        kvstoreHashtableDelete(server.pubsubshard_channels, slot, channel);
    }
    kvstoreReleaseHashtableIterator(kvs_di);
}

/* Remove all conflated shard subscriptions in a slot. Mirrors the stock
 * pubsubShardUnsubscribeAllChannelsInSlot, but for the conflated index. */
void pubsubConflatedShardUnsubscribeAllChannelsInSlot(unsigned int slot) {
    if (!kvstoreHashtableSize(server.pubsub_conflated_shard_channels, slot)) return;

    kvstoreHashtableIterator *kvs_di =
        kvstoreGetHashtableIterator(server.pubsub_conflated_shard_channels, slot, HASHTABLE_ITER_SAFE);
    void *element;
    while (kvstoreHashtableIteratorNext(kvs_di, &element)) {
        hashtable *clients = element;
        robj *channel = *(robj **)hashtableMetadata(clients);
        hashtableIterator client_iter;
        hashtableInitIterator(&client_iter, clients, 0);
        void *next_client;
        while (hashtableNext(&client_iter, &next_client)) {
            client *c = next_client;
            conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_shard_channels, channel);
            if (sub != NULL) conflatedClearPayload(c, sub);
            if (dictDelete(c->pubsub_data->conflated_shard_channels, channel) == DICT_OK) {
                server.pubsub_conflated_shard_subscriptions--;
                addReplyPubsubUnsubscribed(c, channel, pubSubShardType);
                if (clientTotalPubSubSubscriptionCount(c) == 0) unmarkClientAsPubSub(c);
            }
        }
        hashtableCleanupIterator(&client_iter);
        kvstoreHashtableDelete(server.pubsub_conflated_shard_channels, slot, channel);
    }
    kvstoreReleaseHashtableIterator(kvs_di);
}

/* Subscribe a client to a pattern. Returns true if the operation succeeded,
 * or false if the client was already subscribed to that pattern. */
bool pubsubSubscribePattern(client *c, robj *pattern) {
    if (!c->pubsub_data) initClientPubSubData(c);

    /* Add the pattern to the client -> patterns hash table */
    hashtablePosition position;
    bool pattern_added = hashtableFindPositionForInsert(c->pubsub_data->pubsub_patterns, pattern, &position, NULL);
    if (pattern_added) {
        /* Add the client to the pattern -> list of clients hash table */
        hashtable *clients;
        dictEntry *de = dictFind(server.pubsub_patterns, pattern);
        if (de == NULL) {
            clients = hashtableCreate(&clientHashtableType);
            dictAdd(server.pubsub_patterns, pattern, clients);
            incrRefCount(pattern);
        } else {
            pattern = dictGetKey(de);
            clients = dictGetVal(de);
        }
        serverAssert(hashtableAdd(clients, c));
        hashtableInsertAtPosition(c->pubsub_data->pubsub_patterns, pattern, &position);
        incrRefCount(pattern);
        c->pubsub_data->pubsub_object_mem += getStringObjectMemory(pattern);
    }
    /* Notify the client */
    addReplyPubsubPatSubscribed(c, pattern);
    return pattern_added;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribePattern(client *c, robj *pattern, int notify) {
    if (!c->pubsub_data) initClientPubSubData(c);

    incrRefCount(pattern); /* Protect the object. May be the same we remove */
    void *popped = NULL;
    int pattern_deleted = hashtablePop(c->pubsub_data->pubsub_patterns, pattern, &popped);
    if (pattern_deleted) {
        /* Remove the client from the pattern -> clients list hash table */
        dictEntry *de = dictFind(server.pubsub_patterns, pattern);
        serverAssertWithInfo(c, NULL, de != NULL);
        hashtable *clients = dictGetVal(de);
        serverAssertWithInfo(c, NULL, hashtableDelete(clients, c));
        if (hashtableSize(clients) == 0) {
            /* Free the clients hashtable if this was the last client. */
            dictDelete(server.pubsub_patterns, pattern);
        }
        c->pubsub_data->pubsub_object_mem -= getStringObjectMemory(popped);
        decrRefCount(popped);
    }
    /* Notify the client */
    if (notify) addReplyPubsubPatUnsubscribed(c, pattern);
    decrRefCount(pattern);
    return pattern_deleted;
}

/* Subscribe a client to a channel with conflation. The subscription lives in
 * the same client-side index shape as a plain one; only delivery differs. */
int pubsubSubscribeConflatedChannel(client *c, robj *channel, long long interval_ms) {
    if (interval_ms <= 0) {
        addReplyError(c, "CSUBSCRIBE is disabled: set conflated-pubsub-interval-ms to a positive value");
        return 0;
    }
    if (!c->pubsub_data) initClientPubSubData(c);

    int added = 0;
    conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_channels, channel);
    if (sub == NULL) {
        sub = zmalloc(sizeof(*sub));
        sub->interval_ms = interval_ms;
        sub->due = 0;
        sub->payload = NULL;
        serverAssert(dictAdd(c->pubsub_data->conflated_channels, channel, sub) == DICT_OK);
        incrRefCount(channel);

        dictEntry *de = dictFind(server.pubsub_conflated_channels, channel);
        hashtable *clients;
        if (de == NULL) {
            clients = hashtableCreate(&clientHashtableType);
            serverAssert(dictAdd(server.pubsub_conflated_channels, channel, clients) == DICT_OK);
            incrRefCount(channel);
        } else {
            clients = dictGetVal(de);
        }
        serverAssert(hashtableAdd(clients, c));
        server.pubsub_conflated_subscriptions++;
        added = 1;
    }
    /* A regular "subscribe" frame: existing Pub/Sub decoders keep working. */
    addReplyPubsubSubscribed(c, channel, pubSubType);
    return added;
}

/* Unsubscribe a client from a conflated channel. Returns 1 when the
 * subscription existed, 0 otherwise. Mirrors pubsubUnsubscribeChannel. */
int pubsubUnsubscribeConflatedChannel(client *c, robj *channel, int notify) {
    if (!c->pubsub_data) initClientPubSubData(c);

    int deleted = 0;
    conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_channels, channel);
    if (sub != NULL) conflatedClearPayload(c, sub);
    incrRefCount(channel); /* dictDelete may drop the last ref to the key. */
    if (dictDelete(c->pubsub_data->conflated_channels, channel) == DICT_OK) {
        deleted = 1;
        dictEntry *de = dictFind(server.pubsub_conflated_channels, channel);
        serverAssert(de != NULL);
        hashtable *clients = dictGetVal(de);
        serverAssert(hashtableDelete(clients, c));
        if (hashtableSize(clients) == 0) dictDelete(server.pubsub_conflated_channels, channel);
        server.pubsub_conflated_subscriptions--;
    }
    if (notify) addReplyPubsubUnsubscribed(c, channel, pubSubType);
    decrRefCount(channel);
    return deleted;
}

/* Unsubscribe from all conflated channels. Returns the number removed. */
int pubsubUnsubscribeAllConflatedChannels(client *c, int notify) {
    if (!c->pubsub_data) {
        if (notify) {
            initClientPubSubData(c);
            addReplyPubsubUnsubscribed(c, NULL, pubSubType);
        }
        return 0;
    }

    int count = 0;
    dictIterator *di = dictGetSafeIterator(c->pubsub_data->conflated_channels);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        count += pubsubUnsubscribeConflatedChannel(c, dictGetKey(de), notify);
    }
    dictReleaseIterator(di);

    if (notify && count == 0) addReplyPubsubUnsubscribed(c, NULL, pubSubType);
    return count;
}

/* Subscribe a client to a pattern with conflation. Each matching concrete
 * channel gets its own latest-value slot, so `feed:*` delivers the last value
 * of every feed channel independently. */
int pubsubSubscribeConflatedPattern(client *c, robj *pattern, long long interval_ms) {
    if (interval_ms <= 0) {
        addReplyError(c, "CPSUBSCRIBE is disabled: set conflated-pubsub-interval-ms to a positive value");
        return 0;
    }
    if (!c->pubsub_data) initClientPubSubData(c);

    int added = 0;
    conflatedPatternSub *ps = dictFetchValue(c->pubsub_data->conflated_patterns, pattern);
    if (ps == NULL) {
        ps = zmalloc(sizeof(*ps));
        ps->interval_ms = interval_ms;
        ps->channels = dictCreate(&conflatedSubDictType);
        serverAssert(dictAdd(c->pubsub_data->conflated_patterns, pattern, ps) == DICT_OK);
        incrRefCount(pattern);

        dictEntry *de = dictFind(server.pubsub_conflated_patterns, pattern);
        hashtable *clients;
        if (de == NULL) {
            clients = hashtableCreate(&clientHashtableType);
            serverAssert(dictAdd(server.pubsub_conflated_patterns, pattern, clients) == DICT_OK);
            incrRefCount(pattern);
        } else {
            clients = dictGetVal(de);
        }
        serverAssert(hashtableAdd(clients, c));
        server.pubsub_conflated_subscriptions++;
        added = 1;
    }
    addReplyPubsubPatSubscribed(c, pattern);
    return added;
}

/* Unsubscribe a client from a conflated pattern. Returns 1 when the
 * subscription existed, 0 otherwise. */
int pubsubUnsubscribeConflatedPattern(client *c, robj *pattern, int notify) {
    if (!c->pubsub_data) initClientPubSubData(c);

    int deleted = 0;
    conflatedPatternSub *ps = dictFetchValue(c->pubsub_data->conflated_patterns, pattern);
    dict *channels = ps ? ps->channels : NULL;
    if (channels != NULL) {
        dictIterator *cdi = dictGetSafeIterator(channels);
        dictEntry *cde;
        while ((cde = dictNext(cdi)) != NULL) {
            conflatedSub *sub = dictGetVal(cde);
            if (sub->payload != NULL) conflatedClearPayload(c, sub);
            conflatedAccountPatternEntry(c, 0);
        }
        dictReleaseIterator(cdi);
    }
    incrRefCount(pattern); /* dictDelete may drop the last ref to the key. */
    if (dictDelete(c->pubsub_data->conflated_patterns, pattern) == DICT_OK) {
        deleted = 1;
        dictEntry *de = dictFind(server.pubsub_conflated_patterns, pattern);
        serverAssert(de != NULL);
        hashtable *clients = dictGetVal(de);
        serverAssert(hashtableDelete(clients, c));
        if (hashtableSize(clients) == 0) dictDelete(server.pubsub_conflated_patterns, pattern);
        server.pubsub_conflated_subscriptions--;
    }
    if (notify) addReplyPubsubPatUnsubscribed(c, pattern);
    decrRefCount(pattern);
    return deleted;
}

/* Unsubscribe from all conflated patterns. Returns the number removed. */
int pubsubUnsubscribeAllConflatedPatterns(client *c, int notify) {
    if (!c->pubsub_data) {
        if (notify) {
            initClientPubSubData(c);
            addReplyPubsubPatUnsubscribed(c, NULL);
        }
        return 0;
    }

    int count = 0;
    dictIterator *di = dictGetSafeIterator(c->pubsub_data->conflated_patterns);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        count += pubsubUnsubscribeConflatedPattern(c, dictGetKey(de), notify);
    }
    dictReleaseIterator(di);

    if (notify && count == 0) addReplyPubsubPatUnsubscribed(c, NULL);
    return count;
}

/* Subscribe a client to a sharded channel with conflation. The server index is
 * slot-aware, like the stock sharded index, so cluster routing and slot
 * cleanup keep working. */
int pubsubSubscribeConflatedShardChannel(client *c, robj *channel, long long interval_ms) {
    if (interval_ms <= 0) {
        addReplyError(c, "CSSUBSCRIBE is disabled: set conflated-pubsub-interval-ms to a positive value");
        return 0;
    }
    if (!c->pubsub_data) initClientPubSubData(c);

    int added = 0;
    conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_shard_channels, channel);
    if (sub == NULL) {
        sub = zmalloc(sizeof(*sub));
        sub->interval_ms = interval_ms;
        sub->due = 0;
        sub->payload = NULL;

        unsigned int slot = 0;
        if (server.cluster_enabled) slot = getCachedKeySlot(objectGetVal(channel));

        hashtablePosition pos;
        void *existing = NULL;
        hashtable *clients;
        if (!kvstoreHashtableFindPositionForInsert(server.pubsub_conflated_shard_channels, slot, channel, &pos,
                                                   &existing)) {
            clients = existing;
            channel = *(robj **)hashtableMetadata(clients);
        } else {
            clients = hashtableCreate(&clientHashtableType);
            *(robj **)hashtableMetadata(clients) = channel;
            incrRefCount(channel);
            kvstoreHashtableInsertAtPosition(server.pubsub_conflated_shard_channels, slot, clients, &pos);
        }

        serverAssert(dictAdd(c->pubsub_data->conflated_shard_channels, channel, sub) == DICT_OK);
        incrRefCount(channel);
        serverAssert(hashtableAdd(clients, c));
        server.pubsub_conflated_shard_subscriptions++;
        added = 1;
    }
    addReplyPubsubSubscribed(c, channel, pubSubShardType);
    return added;
}

/* Unsubscribe a client from a conflated shard channel. */
int pubsubUnsubscribeConflatedShardChannel(client *c, robj *channel, int notify) {
    if (!c->pubsub_data) initClientPubSubData(c);

    int deleted = 0;
    conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_shard_channels, channel);
    if (sub != NULL) conflatedClearPayload(c, sub);
    incrRefCount(channel); /* dictDelete may drop the last ref to the key. */
    if (dictDelete(c->pubsub_data->conflated_shard_channels, channel) == DICT_OK) {
        deleted = 1;
        unsigned int slot = 0;
        if (server.cluster_enabled) slot = keyHashSlot(objectGetVal(channel), (int)sdslen(objectGetVal(channel)));
        void *found = NULL;
        kvstoreHashtableFind(server.pubsub_conflated_shard_channels, slot, channel, &found);
        serverAssert(found != NULL);
        hashtable *clients = found;
        serverAssert(hashtableDelete(clients, c));
        if (hashtableSize(clients) == 0) {
            kvstoreHashtableDelete(server.pubsub_conflated_shard_channels, slot, channel);
        }
        server.pubsub_conflated_shard_subscriptions--;
    }
    if (notify) addReplyPubsubUnsubscribed(c, channel, pubSubShardType);
    decrRefCount(channel);
    return deleted;
}

/* Unsubscribe from all conflated shard channels. Returns the number removed. */
int pubsubUnsubscribeAllConflatedShardChannels(client *c, int notify) {
    if (!c->pubsub_data) {
        if (notify) {
            initClientPubSubData(c);
            addReplyPubsubUnsubscribed(c, NULL, pubSubShardType);
        }
        return 0;
    }

    int count = 0;
    dictIterator *di = dictGetSafeIterator(c->pubsub_data->conflated_shard_channels);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        count += pubsubUnsubscribeConflatedShardChannel(c, dictGetKey(de), notify);
    }
    dictReleaseIterator(di);

    if (notify && count == 0) addReplyPubsubUnsubscribed(c, NULL, pubSubShardType);
    return count;
}

/* Unsubscribe from all the channels. Return the number of channels the
 * client was subscribed to. */
int pubsubUnsubscribeAllChannelsInternal(client *c, int notify, pubsubtype type) {
    int count = 0;
    if (hashtableSize(type.clientPubSubChannels(c)) > 0) {
        hashtableIterator iter;
        hashtableInitIterator(&iter, type.clientPubSubChannels(c), HASHTABLE_ITER_SAFE);
        void *channel;
        while (hashtableNext(&iter, &channel)) {
            count += pubsubUnsubscribeChannel(c, channel, notify, type);
        }
        hashtableCleanupIterator(&iter);
    }
    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) {
        addReplyPubsubUnsubscribed(c, NULL, type);
    }
    return count;
}

/*
 * Unsubscribe a client from all global channels.
 */
int pubsubUnsubscribeAllChannels(client *c, int notify) {
    int count = pubsubUnsubscribeAllChannelsInternal(c, notify, pubSubType);
    return count;
}

/*
 * Unsubscribe a client from all shard subscribed channels.
 */
int pubsubUnsubscribeShardAllChannels(client *c, int notify) {
    int count = pubsubUnsubscribeAllChannelsInternal(c, notify, pubSubShardType);
    return count;
}

/* Unsubscribe from all the patterns. Return the number of patterns the
 * client was subscribed from. */
int pubsubUnsubscribeAllPatterns(client *c, int notify) {
    int count = 0;
    if (!c->pubsub_data) initClientPubSubData(c);

    if (hashtableSize(c->pubsub_data->pubsub_patterns) > 0) {
        hashtableIterator iter;
        hashtableInitIterator(&iter, c->pubsub_data->pubsub_patterns, HASHTABLE_ITER_SAFE);

        void *pattern;
        while (hashtableNext(&iter, &pattern)) {
            count += pubsubUnsubscribePattern(c, pattern, notify);
        }

        hashtableCleanupIterator(&iter);
    }

    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) addReplyPubsubPatUnsubscribed(c, NULL);
    return count;
}

/*
 * Publish a message to all the subscribers.
 */
int pubsubPublishMessageInternal(robj *channel, robj *message, pubsubtype type) {
    int receivers = 0;
    void *element;
    dictEntry *de;
    dictIterator *di;
    int slot = -1;

    /* Send to clients listening for that channel */
    if (server.cluster_enabled && type.shard) {
        slot = keyHashSlot(objectGetVal(channel), sdslen(objectGetVal(channel)));
    }
    if (kvstoreHashtableFind(*type.serverPubSubChannels, (slot == -1) ? 0 : slot, channel, &element)) {
        hashtable *clients = element;
        hashtableIterator iter;
        hashtableInitIterator(&iter, clients, 0);
        void *c;
        while (hashtableNext(&iter, &c)) {
            addReplyPubsubMessage(c, channel, message, *type.messageBulk);
            clusterSlotStatsAddNetworkBytesOutForShardedPubSubInternalPropagation(c, slot);
            updateClientMemUsageAndBucket(c);
            receivers++;
        }
        hashtableCleanupIterator(&iter);
    }

    if (type.shard) {
        /* Shard pubsub ignores patterns, but conflated shard subscribers still
         * accept the value and count as receivers. */
        if (kvstoreSize(server.pubsub_conflated_shard_channels) > 0) {
            receivers += pubsubPublishConflatedShardedChannel(channel, message, slot);
        }
        return receivers;
    }

    /* A matching conflated subscriber accepted the value into its latest-value
     * slot, so it counts as a PUBLISH receiver even though delivery is
     * deferred. The size checks keep the no-conflation path to two compares. */
    if (dictSize(server.pubsub_conflated_channels) > 0) {
        receivers += pubsubPublishConflatedChannel(channel, message);
    }
    if (dictSize(server.pubsub_conflated_patterns) > 0) {
        robj *decoded = getDecodedObject(channel);
        receivers += pubsubPublishConflatedPatterns(decoded, message);
        decrRefCount(decoded);
    }

    /* Send to clients listening to matching channels */
    di = dictGetIterator(server.pubsub_patterns);
    if (di) {
        channel = getDecodedObject(channel);
        while ((de = dictNext(di)) != NULL) {
            robj *pattern = dictGetKey(de);
            hashtable *clients = dictGetVal(de);
            if (!stringmatchlen((char *)objectGetVal(pattern), sdslen(objectGetVal(pattern)),
                                (char *)objectGetVal(channel), sdslen(objectGetVal(channel)), 0))
                continue;

            hashtableIterator iter;
            hashtableInitIterator(&iter, clients, 0);
            void *c;
            while (hashtableNext(&iter, &c)) {
                addReplyPubsubPatMessage(c, pattern, channel, message);
                updateClientMemUsageAndBucket(c);
                receivers++;
            }
            hashtableCleanupIterator(&iter);
        }
        decrRefCount(channel);
        dictReleaseIterator(di);
    }
    return receivers;
}

/* Publish a message to all the subscribers. */
int pubsubPublishMessage(robj *channel, robj *message, int sharded) {
    return pubsubPublishMessageInternal(channel, message, sharded ? pubSubShardType : pubSubType);
}

/* Store `message` as the latest pending value of every conflated subscriber of
 * `channel`. Replacements keep the original due time, so the cadence is
 * periodic and does not slide with the arrival rate. */
int pubsubPublishConflatedChannel(robj *channel, robj *message) {
    dictEntry *de = dictFind(server.pubsub_conflated_channels, channel);
    if (de == NULL) return 0;

    hashtable *clients = dictGetVal(de);
    int receivers = 0;
    mstime_t now = mstime();
    hashtableIterator iter;
    hashtableInitIterator(&iter, clients, 0);
    void *entry;
    while (hashtableNext(&iter, &entry)) {
        client *c = entry;
        if (c->pubsub_data == NULL) continue;
        conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_channels, channel);
        if (sub == NULL) continue;
        size_t old_bytes = conflatedPayloadBytes(sub->payload);
        size_t new_bytes = conflatedPayloadBytes(message);
        size_t delta = new_bytes > old_bytes ? new_bytes - old_bytes : 0;
        if (!conflatedReserve(c, delta, sub->payload == NULL, 0, 0, NULL)) {
            server.pubsub_conflated_dropped++;
            continue;
        }
        /* Reserve may have cleared this subscription's old payload. */
        int had_payload = sub->payload != NULL;
        old_bytes = conflatedPayloadBytes(sub->payload);
        if (had_payload) {
            decrRefCount(sub->payload);
            conflatedAccountPayload(c, old_bytes, 0);
            server.pubsub_conflated_replaced++;
        }
        sub->payload = message;
        incrRefCount(message);
        conflatedAccountPayload(c, new_bytes, 1);
        if (!had_payload) {
            sub->due = now + sub->interval_ms;
            conflatedMarkDue(sub->due);
        }
        receivers++;
    }
    hashtableCleanupIterator(&iter);
    return receivers;
}

/* Store `message` as the latest pending value of every conflated shard
 * subscriber of `channel`. `slot` is the channel's cluster slot, or -1 when
 * the cluster is disabled. */
int pubsubPublishConflatedShardedChannel(robj *channel, robj *message, int slot) {
    void *found = NULL;
    if (!kvstoreHashtableFind(server.pubsub_conflated_shard_channels, slot < 0 ? 0 : slot, channel, &found)) return 0;

    hashtable *clients = found;
    int receivers = 0;
    mstime_t now = mstime();
    hashtableIterator iter;
    hashtableInitIterator(&iter, clients, 0);
    void *entry;
    while (hashtableNext(&iter, &entry)) {
        client *c = entry;
        if (c->pubsub_data == NULL) continue;
        conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_shard_channels, channel);
        if (sub == NULL) continue;
        size_t old_bytes = conflatedPayloadBytes(sub->payload);
        size_t new_bytes = conflatedPayloadBytes(message);
        size_t delta = new_bytes > old_bytes ? new_bytes - old_bytes : 0;
        if (!conflatedReserve(c, delta, sub->payload == NULL, 0, 0, NULL)) {
            server.pubsub_conflated_dropped++;
            continue;
        }
        int had_payload = sub->payload != NULL;
        old_bytes = conflatedPayloadBytes(sub->payload);
        if (had_payload) {
            decrRefCount(sub->payload);
            conflatedAccountPayload(c, old_bytes, 0);
            server.pubsub_conflated_replaced++;
        }
        sub->payload = message;
        incrRefCount(message);
        conflatedAccountPayload(c, new_bytes, 1);
        if (!had_payload) {
            sub->due = now + sub->interval_ms;
            conflatedMarkDue(sub->due);
        }
        receivers++;
    }
    hashtableCleanupIterator(&iter);
    return receivers;
}

/* Store `message` as the latest pending value of every conflated pattern
 * subscription matching `channel`. The pending slot is per (pattern, concrete
 * channel), since one pattern matches many channels. */
int pubsubPublishConflatedPatterns(robj *channel, robj *message) {
    dictIterator *di = dictGetIterator(server.pubsub_conflated_patterns);
    dictEntry *de;
    mstime_t now = mstime();
    int receivers = 0;
    while ((de = dictNext(di)) != NULL) {
        robj *pattern = dictGetKey(de);
        if (!stringmatchlen((char *)objectGetVal(pattern), sdslen(objectGetVal(pattern)), (char *)objectGetVal(channel),
                            sdslen(objectGetVal(channel)), 0))
            continue;

        hashtable *clients = dictGetVal(de);
        hashtableIterator iter;
        hashtableInitIterator(&iter, clients, 0);
        void *entry;
        while (hashtableNext(&iter, &entry)) {
            client *c = entry;
            if (c->pubsub_data == NULL) continue;
            conflatedPatternSub *ps = dictFetchValue(c->pubsub_data->conflated_patterns, pattern);
            if (ps == NULL) continue;
            dict *channels = ps->channels;

            conflatedSub *sub = dictFetchValue(channels, channel);
            int new_entry = sub == NULL;
            size_t old_bytes = new_entry ? 0 : conflatedPayloadBytes(sub->payload);
            size_t new_bytes = conflatedPayloadBytes(message);
            size_t delta = new_bytes > old_bytes ? new_bytes - old_bytes : 0;
            int new_slot = new_entry || sub->payload == NULL;
            if (!conflatedReserve(c, delta, new_slot, new_entry, new_entry ? 0 : 1, sub)) {
                server.pubsub_conflated_dropped++;
                continue;
            }
            if (new_entry) {
                sub = zmalloc(sizeof(*sub));
                sub->interval_ms = ps->interval_ms;
                sub->due = 0;
                sub->payload = NULL;
                serverAssert(dictAdd(channels, channel, sub) == DICT_OK);
                incrRefCount(channel);
                conflatedAccountPatternEntry(c, 1);
            }
            int had_payload = sub->payload != NULL;
            old_bytes = conflatedPayloadBytes(sub->payload);
            if (had_payload) {
                decrRefCount(sub->payload);
                conflatedAccountPayload(c, old_bytes, 0);
                server.pubsub_conflated_replaced++;
            }
            sub->payload = message;
            incrRefCount(message);
            conflatedAccountPayload(c, new_bytes, 1);
            if (!had_payload) {
                sub->due = now + sub->interval_ms;
                conflatedMarkDue(sub->due);
            }
            receivers++;
        }
        hashtableCleanupIterator(&iter);
    }
    dictReleaseIterator(di);
    return receivers;
}

/* Deliver pending conflated shard payloads as smessage frames. */
static void pubsubConflatedShardCron(mstime_t now, size_t *budget) {
    kvstoreIterator *kvs_it = kvstoreIteratorInit(server.pubsub_conflated_shard_channels, 0);
    void *element;
    while (kvstoreIteratorNext(kvs_it, &element) && *budget > 0) {
        hashtable *clients = element;
        robj *channel = *(robj **)hashtableMetadata(clients);
        hashtableIterator iter;
        hashtableInitIterator(&iter, clients, 0);
        void *entry;
        while (hashtableNext(&iter, &entry) && *budget > 0) {
            client *c = entry;
            if (c->pubsub_data == NULL) continue;
            conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_shard_channels, channel);
            if (sub == NULL || sub->payload == NULL || now < sub->due) continue;

            robj *payload = sub->payload;
            size_t bytes = conflatedPayloadBytes(payload);
            sub->payload = NULL;
            sub->due = 0;
            addReplyPubsubMessage(c, channel, payload, shared.smessagebulk);
            updateClientMemUsageAndBucket(c);
            decrRefCount(payload);
            conflatedAccountPayload(c, bytes, 0);
            server.pubsub_conflated_delivered++;
            (*budget)--;
        }
        hashtableCleanupIterator(&iter);
    }
    kvstoreIteratorRelease(kvs_it);
}

/* Deliver pending conflated pattern payloads. Each entry carries its own
 * concrete channel, so a pattern's channels are delivered independently. */
static void pubsubConflatedPatternsCron(mstime_t now, size_t *budget) {
    dictIterator *pdi = dictGetIterator(server.pubsub_conflated_patterns);
    dictEntry *pde;
    while ((pde = dictNext(pdi)) != NULL && *budget > 0) {
        robj *pattern = dictGetKey(pde);
        hashtable *clients = dictGetVal(pde);
        hashtableIterator iter;
        hashtableInitIterator(&iter, clients, 0);
        void *entry;
        while (hashtableNext(&iter, &entry) && *budget > 0) {
            client *c = entry;
            if (c->pubsub_data == NULL) continue;
            conflatedPatternSub *ps = dictFetchValue(c->pubsub_data->conflated_patterns, pattern);
            if (ps == NULL) continue;
            dict *channels = ps->channels;

            dictIterator *cdi = dictGetSafeIterator(channels);
            dictEntry *cde;
            while ((cde = dictNext(cdi)) != NULL && *budget > 0) {
                robj *channel = dictGetKey(cde);
                conflatedSub *sub = dictGetVal(cde);
                if (sub->payload == NULL || now < sub->due) continue;

                robj *payload = sub->payload;
                size_t bytes = conflatedPayloadBytes(payload);
                sub->payload = NULL;
                sub->due = 0;
                addReplyPubsubPatMessage(c, pattern, channel, payload);
                updateClientMemUsageAndBucket(c);
                decrRefCount(payload);
                conflatedAccountPayload(c, bytes, 0);
                server.pubsub_conflated_delivered++;
                (*budget)--;
            }
            dictReleaseIterator(cdi);
        }
        hashtableCleanupIterator(&iter);
    }
    dictReleaseIterator(pdi);
}

/* Deliver every pending conflated payload whose due time has passed, up to the
 * configured budget. The dedicated timer continues the work on its next tick. */
void pubsubConflatedCron(void) {
    if (server.pubsub_conflated_interval_ms <= 0) return;
    int has_channels = dictSize(server.pubsub_conflated_channels) > 0;
    int has_patterns = dictSize(server.pubsub_conflated_patterns) > 0;
    int has_shard = kvstoreSize(server.pubsub_conflated_shard_channels) > 0;
    if (!has_channels && !has_patterns && !has_shard) return;

    size_t budget = (size_t)server.pubsub_conflated_flush_budget;
    if (budget == 0) budget = 1;
    mstime_t now = mstime();

    /* With a known future due time, skip the full scan entirely. */
    if (server.pubsub_conflated_next_due == LLONG_MAX) return;
    if (server.pubsub_conflated_next_due != 0 && now < server.pubsub_conflated_next_due) return;

    if (has_channels) {
        dictIterator *di = dictGetIterator(server.pubsub_conflated_channels);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL && budget > 0) {
            robj *channel = dictGetKey(de);
            hashtable *clients = dictGetVal(de);
            hashtableIterator iter;
            hashtableInitIterator(&iter, clients, 0);
            void *entry;
            while (hashtableNext(&iter, &entry) && budget > 0) {
                client *c = entry;
                if (c->pubsub_data == NULL) continue;
                conflatedSub *sub = dictFetchValue(c->pubsub_data->conflated_channels, channel);
                if (sub == NULL || sub->payload == NULL || now < sub->due) continue;

                robj *payload = sub->payload;
                size_t bytes = conflatedPayloadBytes(payload);
                sub->payload = NULL;
                sub->due = 0;
                addReplyPubsubMessage(c, channel, payload, shared.messagebulk);
                updateClientMemUsageAndBucket(c);
                decrRefCount(payload);
                conflatedAccountPayload(c, bytes, 0);
                server.pubsub_conflated_delivered++;
                budget--;
            }
            hashtableCleanupIterator(&iter);
        }
        dictReleaseIterator(di);
    }

    if (has_shard && budget > 0) pubsubConflatedShardCron(now, &budget);
    if (has_patterns && budget > 0) pubsubConflatedPatternsCron(now, &budget);
    if (budget == 0) server.pubsub_conflated_flush_truncated++;
    conflatedRecomputeNextDue();
}

/* Dedicated delivery timer. Ticking at a quarter of the interval keeps the
 * delivery jitter bounded without waking the loop at hz 100. */
long long pubsubConflatedTimer(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);
    pubsubConflatedCron();
    int interval = server.pubsub_conflated_interval_ms;
    if (interval <= 0) return 1000;
    long long tick = interval / 4;
    if (tick < 1) tick = 1;
    /* Bound the re-arm delay so CONFIG SET of a smaller interval takes effect
     * promptly; with next_due, a 1 s wakeup is cheap when nothing is due. */
    if (tick > 1000) tick = 1000;
    return tick;
}

/*-----------------------------------------------------------------------------
 * Pubsub commands implementation
 *----------------------------------------------------------------------------*/

/* Parse an entry of the form name=interval_ms. Returns the name length and
 * sets *interval_ms (0 when the suffix is absent or not a positive integer). */
static size_t conflatedEntryNameLen(const char *entry, size_t len, long long *interval_ms) {
    *interval_ms = 0;
    size_t eq = len;
    for (size_t i = len; i > 0; i--) {
        if (entry[i - 1] == '=') {
            eq = i - 1;
            break;
        }
    }
    if (eq == len || eq == 0 || eq == len - 1) return len;

    long long value = 0;
    for (size_t i = eq + 1; i < len; i++) {
        if (entry[i] < '0' || entry[i] > '9') return len;
        value = value * 10 + (entry[i] - '0');
        if (value > INT_MAX) return len;
    }
    if (value <= 0) return len;
    *interval_ms = value;
    return eq;
}

/* Returns -1 when the value is not listed, otherwise the entry interval
 * (0 when the entry has no explicit =ms suffix). */
static long long conflatedListContains(const char *list, const char *value) {
    if (list == NULL || *list == '\0') return -1;
    size_t value_len = strlen(value);
    const char *p = list;
    while (1) {
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        long long interval = 0;
        size_t name_len = conflatedEntryNameLen(p, len, &interval);
        if (name_len == value_len && memcmp(p, value, name_len) == 0) return interval;
        if (comma == NULL) break;
        p = comma + 1;
    }
    return -1;
}

static long long conflatedListMatches(const char *list, robj *expression) {
    if (list == NULL || *list == '\0') return -1;
    robj *decoded = getDecodedObject(expression);
    long long result = -1;
    const char *p = list;
    while (1) {
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        long long interval = 0;
        size_t name_len = conflatedEntryNameLen(p, len, &interval);
        if (name_len > 0 &&
            stringmatchlen(p, (int)name_len, objectGetVal(decoded), (int)sdslen(objectGetVal(decoded)), 0)) {
            result = interval;
            break;
        }
        if (comma == NULL) break;
        p = comma + 1;
    }
    decrRefCount(decoded);
    return result;
}

/* Resolve the transparent delivery interval. Returns 0 when the subscription
 * is not auto-conflated. An entry interval overrides the global value, and a
 * matching pattern entry wins over the user entry. */
static long long pubsubAutoConflateInterval(client *c, robj *expression) {
    if (server.pubsub_conflated_interval_ms <= 0) return 0;

    /* The rule table is authoritative when present: ordered, first match wins.
     * A non-match stays stock; the single-line lists are read only when the
     * table is empty. */
    if (listLength(server.pubsub_conflated_auto_rules) > 0) {
        const char *name = (c->user && c->user->name) ? c->user->name : "";
        robj *decoded = getDecodedObject(expression);
        listIter li;
        listNode *ln;
        listRewind(server.pubsub_conflated_auto_rules, &li);
        while ((ln = listNext(&li)) != NULL) {
            conflatedAutoRule *rule = listNodeValue(ln);
            int any_user = (rule->user[0] == '*' && rule->user[1] == '\0');
            if (!any_user && strcmp(rule->user, name) != 0) continue;
            if (stringmatchlen(rule->pattern, sdslen(rule->pattern), objectGetVal(decoded),
                               sdslen(objectGetVal(decoded)), 0)) {
                long long interval = rule->interval_ms;
                decrRefCount(decoded);
                return interval;
            }
        }
        decrRefCount(decoded);
        return 0;
    }

    const char *users = server.pubsub_conflated_auto_users;
    const char *patterns = server.pubsub_conflated_auto_patterns;
    int has_users = users != NULL && *users != '\0';
    int has_patterns = patterns != NULL && *patterns != '\0';
    if (!has_users && !has_patterns) return 0;

    long long user_interval = 0;
    if (has_users) {
        const char *name = (c->user && c->user->name) ? c->user->name : "";
        user_interval = conflatedListContains(users, name);
        if (user_interval < 0) return 0;
    }
    long long pattern_interval = 0;
    if (has_patterns) {
        pattern_interval = conflatedListMatches(patterns, expression);
        if (pattern_interval < 0) return 0;
    }
    if (pattern_interval > 0) return pattern_interval;
    if (user_interval > 0) return user_interval;
    return server.pubsub_conflated_interval_ms;
}

/* Remove a channel from both the plain and the conflated subscription sets,
 * emitting a single ack. Used by the normal UNSUBSCRIBE command so a client
 * that entered transparent mode can leave it with the stock command. */
static int pubsubUnsubscribeAutoChannel(client *c, robj *channel, int notify) {
    int removed = 0;
    if (!c->pubsub_data) initClientPubSubData(c);
    incrRefCount(channel); /* the subscription indexes borrow the key */
    if (dictFind(c->pubsub_data->conflated_channels, channel)) {
        removed += pubsubUnsubscribeConflatedChannel(c, channel, 0);
    }
    void *found = NULL;
    if (hashtableFind(c->pubsub_data->pubsub_channels, channel, &found)) {
        removed += pubsubUnsubscribeChannel(c, channel, 0, pubSubType);
    }
    if (notify) addReplyPubsubUnsubscribed(c, channel, pubSubType);
    decrRefCount(channel);
    return removed;
}

/* Shard counterpart of pubsubUnsubscribeAutoChannel. */
static int pubsubUnsubscribeAutoShardChannel(client *c, robj *channel, int notify) {
    int removed = 0;
    if (!c->pubsub_data) initClientPubSubData(c);
    incrRefCount(channel); /* the subscription indexes borrow the key */
    if (dictFind(c->pubsub_data->conflated_shard_channels, channel)) {
        removed += pubsubUnsubscribeConflatedShardChannel(c, channel, 0);
    }
    void *found = NULL;
    if (hashtableFind(c->pubsub_data->pubsubshard_channels, channel, &found)) {
        removed += pubsubUnsubscribeChannel(c, channel, 0, pubSubShardType);
    }
    if (notify) addReplyPubsubUnsubscribed(c, channel, pubSubShardType);
    decrRefCount(channel);
    return removed;
}

/* Pattern counterpart of pubsubUnsubscribeAutoChannel. */
static int pubsubUnsubscribeAutoPattern(client *c, robj *pattern, int notify) {
    int removed = 0;
    if (!c->pubsub_data) initClientPubSubData(c);
    incrRefCount(pattern); /* the subscription indexes borrow the key */
    if (dictFind(c->pubsub_data->conflated_patterns, pattern)) {
        removed += pubsubUnsubscribeConflatedPattern(c, pattern, 0);
    }
    void *found = NULL;
    if (hashtableFind(c->pubsub_data->pubsub_patterns, pattern, &found)) {
        removed += pubsubUnsubscribePattern(c, pattern, 0);
    }
    if (notify) addReplyPubsubPatUnsubscribed(c, pattern);
    decrRefCount(pattern);
    return removed;
}

/* SUBSCRIBE channel [channel ...] */
void subscribeCommand(client *c) {
    int j;
    if (c->flag.deny_blocking && !c->flag.multi) {
        /**
         * A client that has CLIENT_DENY_BLOCKING flag on
         * expect a reply per command and so cannot execute subscribe.
         *
         * Notice that we have a special treatment for multi because of
         * backward compatibility
         */
        addReplyError(c, "SUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }
    for (j = 1; j < c->argc; j++) {
        long long interval = pubsubAutoConflateInterval(c, c->argv[j]);
        if (interval > 0) {
            pubsubSubscribeConflatedChannel(c, c->argv[j], interval);
        } else {
            pubsubSubscribeChannel(c, c->argv[j], pubSubType);
        }
    }
    markClientAsPubSub(c);
}

/* UNSUBSCRIBE [channel ...] */
void unsubscribeCommand(client *c) {
    if (!c->pubsub_data) initClientPubSubData(c);

    if (c->argc == 1) {
        int removed = 0;
        dictIterator *di = dictGetSafeIterator(c->pubsub_data->conflated_channels);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL) {
            removed += pubsubUnsubscribeAutoChannel(c, dictGetKey(de), 1);
        }
        dictReleaseIterator(di);

        hashtableIterator iter;
        hashtableInitIterator(&iter, c->pubsub_data->pubsub_channels, HASHTABLE_ITER_SAFE);
        void *channel;
        while (hashtableNext(&iter, &channel)) {
            removed += pubsubUnsubscribeAutoChannel(c, channel, 1);
        }
        hashtableCleanupIterator(&iter);

        if (removed == 0) addReplyPubsubUnsubscribed(c, NULL, pubSubType);
    } else {
        int j;

        for (j = 1; j < c->argc; j++) pubsubUnsubscribeAutoChannel(c, c->argv[j], 1);
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

/* PSUBSCRIBE pattern [pattern ...] */
void psubscribeCommand(client *c) {
    int j;
    if (c->flag.deny_blocking && !c->flag.multi) {
        /**
         * A client that has CLIENT_DENY_BLOCKING flag on
         * expect a reply per command and so cannot execute subscribe.
         *
         * Notice that we have a special treatment for multi because of
         * backward compatibility
         */
        addReplyError(c, "PSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }

    for (j = 1; j < c->argc; j++) {
        long long interval = pubsubAutoConflateInterval(c, c->argv[j]);
        if (interval > 0) {
            pubsubSubscribeConflatedPattern(c, c->argv[j], interval);
        } else {
            pubsubSubscribePattern(c, c->argv[j]);
        }
    }
    markClientAsPubSub(c);
}

/* PUNSUBSCRIBE [pattern [pattern ...]] */
void punsubscribeCommand(client *c) {
    if (!c->pubsub_data) initClientPubSubData(c);

    if (c->argc == 1) {
        int removed = 0;
        dictIterator *di = dictGetSafeIterator(c->pubsub_data->conflated_patterns);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL) {
            removed += pubsubUnsubscribeAutoPattern(c, dictGetKey(de), 1);
        }
        dictReleaseIterator(di);

        hashtableIterator iter;
        hashtableInitIterator(&iter, c->pubsub_data->pubsub_patterns, HASHTABLE_ITER_SAFE);
        void *pattern;
        while (hashtableNext(&iter, &pattern)) {
            removed += pubsubUnsubscribeAutoPattern(c, pattern, 1);
        }
        hashtableCleanupIterator(&iter);

        if (removed == 0) addReplyPubsubPatUnsubscribed(c, NULL);
    } else {
        int j;

        for (j = 1; j < c->argc; j++) pubsubUnsubscribeAutoPattern(c, c->argv[j], 1);
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

/* CSUBSCRIBE channel [channel ...] */
void csubscribeCommand(client *c) {
    if (c->flag.deny_blocking && !c->flag.multi) {
        addReplyError(c, "CSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }
    for (int j = 1; j < c->argc; j++)
        pubsubSubscribeConflatedChannel(c, c->argv[j], server.pubsub_conflated_interval_ms);
    if (c->pubsub_data && clientTotalPubSubSubscriptionCount(c) > 0) markClientAsPubSub(c);
}

/* CUNSUBSCRIBE [channel ...] */
void cunsubscribeCommand(client *c) {
    if (!c->pubsub_data) initClientPubSubData(c);

    if (c->argc == 1) {
        pubsubUnsubscribeAllConflatedChannels(c, 1);
    } else {
        for (int j = 1; j < c->argc; j++) pubsubUnsubscribeConflatedChannel(c, c->argv[j], 1);
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) unmarkClientAsPubSub(c);
}

/* CSSUBSCRIBE shardchannel [shardchannel ...] */
void cssubscribeCommand(client *c) {
    if (c->flag.deny_blocking && !c->flag.multi) {
        addReplyError(c, "CSSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }
    for (int j = 1; j < c->argc; j++)
        pubsubSubscribeConflatedShardChannel(c, c->argv[j], server.pubsub_conflated_interval_ms);
    if (c->pubsub_data && clientTotalPubSubSubscriptionCount(c) > 0) markClientAsPubSub(c);
}

/* CSUNSUBSCRIBE [shardchannel ...] */
void csunsubscribeCommand(client *c) {
    if (!c->pubsub_data) initClientPubSubData(c);

    if (c->argc == 1) {
        pubsubUnsubscribeAllConflatedShardChannels(c, 1);
    } else {
        for (int j = 1; j < c->argc; j++) pubsubUnsubscribeConflatedShardChannel(c, c->argv[j], 1);
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) unmarkClientAsPubSub(c);
}

/* CPSUBSCRIBE pattern [pattern ...] */
void cpsubscribeCommand(client *c) {
    if (c->flag.deny_blocking && !c->flag.multi) {
        addReplyError(c, "CPSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }
    for (int j = 1; j < c->argc; j++)
        pubsubSubscribeConflatedPattern(c, c->argv[j], server.pubsub_conflated_interval_ms);
    if (c->pubsub_data && clientTotalPubSubSubscriptionCount(c) > 0) markClientAsPubSub(c);
}

/* CPUNSUBSCRIBE [pattern ...] */
void cpunsubscribeCommand(client *c) {
    if (!c->pubsub_data) initClientPubSubData(c);

    if (c->argc == 1) {
        pubsubUnsubscribeAllConflatedPatterns(c, 1);
    } else {
        for (int j = 1; j < c->argc; j++) pubsubUnsubscribeConflatedPattern(c, c->argv[j], 1);
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) unmarkClientAsPubSub(c);
}

/* This function wraps pubsubPublishMessage and also propagates the message to cluster.
 * Used by the commands PUBLISH/SPUBLISH and their respective module APIs.
 * Returns zero without publishing if the message exceeds the supported cluster
 * bus publish size. */
int pubsubPublishMessageAndPropagateToCluster(robj *channel, robj *message, int sharded) {
    if (server.cluster_enabled && !clusterPublishMessageLengthValid(channel, message)) return 0;

    int receivers = pubsubPublishMessage(channel, message, sharded);
    if (server.cluster_enabled) clusterPropagatePublish(channel, message, sharded);
    return receivers;
}

/* PUBLISH <channel> <message> */
void publishCommand(client *c) {
    if (server.sentinel_mode) {
        sentinelPublishCommand(c);
        return;
    }

    if (server.cluster_enabled && !clusterPublishMessageLengthValid(c->argv[1], c->argv[2])) {
        addReplyError(c, "Publish message is too large for the cluster bus");
        return;
    }
    int receivers = pubsubPublishMessageAndPropagateToCluster(c->argv[1], c->argv[2], 0);
    if (!server.cluster_enabled) forceCommandPropagation(c, PROPAGATE_REPL);
    addReplyLongLong(c, receivers);
}

/* PUBSUB command for Pub/Sub introspection. */
void pubsubCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(objectGetVal(c->argv[1]), "help")) {
        const char *help[] = {
            "CHANNELS [<pattern>]",
            "    Return the currently active channels matching a <pattern> (default: '*').",
            "NUMPAT",
            "    Return number of subscriptions to patterns.",
            "NUMSUB [<channel> ...]",
            "    Return the number of subscribers for the specified channels, excluding",
            "    pattern subscriptions(default: no channels).",
            "SHARDCHANNELS [<pattern>]",
            "    Return the currently active shard level channels matching a <pattern> (default: '*').",
            "SHARDNUMSUB [<shardchannel> ...]",
            "    Return the number of subscribers for the specified shard level channel(s)",
            "CONFLATED",
            "    Return conflated Pub/Sub counters and memory gauges.",
            "CNUMSUB [<channel> ...]",
            "    Return conflated subscriber counts for the specified channels.",
            "CNUMPAT",
            "    Return the number of conflated pattern subscriptions.",
            "CCHANNELS [<pattern>]",
            "    Return channels with at least one conflated subscriber.",
            "CSHARDCHANNELS [<pattern>]",
            "    Return shard channels with at least one conflated subscriber.",
            "CSHARDNUMSUB [<shardchannel> ...]",
            "    Return conflated subscriber counts for the specified shard channels.",
            NULL};
        addReplyHelp(c, help);
    } else if (!strcasecmp(objectGetVal(c->argv[1]), "channels") && (c->argc == 2 || c->argc == 3)) {
        /* PUBSUB CHANNELS [<pattern>] */
        sds pat = (c->argc == 2) ? NULL : objectGetVal(c->argv[2]);
        channelList(c, pat, server.pubsub_channels);
    } else if (!strcasecmp(objectGetVal(c->argv[1]), "numsub") && c->argc >= 2) {
        /* PUBSUB NUMSUB [Channel_1 ... Channel_N] */
        int j;

        addReplyArrayLen(c, (c->argc - 2) * 2);
        for (j = 2; j < c->argc; j++) {
            void *found = NULL;
            kvstoreHashtableFind(server.pubsub_channels, 0, c->argv[j], &found);
            dict *d = found;
            addReplyBulk(c, c->argv[j]);
            addReplyLongLong(c, d ? dictSize(d) : 0);
        }
    } else if (!strcasecmp(objectGetVal(c->argv[1]), "numpat") && c->argc == 2) {
        /* PUBSUB NUMPAT */
        addReplyLongLong(c, dictSize(server.pubsub_patterns));
    } else if (!strcasecmp(objectGetVal(c->argv[1]), "shardchannels") && (c->argc == 2 || c->argc == 3)) {
        /* PUBSUB SHARDCHANNELS */
        sds pat = (c->argc == 2) ? NULL : objectGetVal(c->argv[2]);
        channelList(c, pat, server.pubsubshard_channels);
    } else if (!strcasecmp(objectGetVal(c->argv[1]), "shardnumsub") && c->argc >= 2) {
        /* PUBSUB SHARDNUMSUB [ShardChannel_1 ... ShardChannel_N] */
        int j;
        addReplyArrayLen(c, (c->argc - 2) * 2);
        for (j = 2; j < c->argc; j++) {
            sds key = objectGetVal(c->argv[j]);
            unsigned int slot = server.cluster_enabled ? keyHashSlot(key, (int)sdslen(key)) : 0;
            void *found = NULL;
            kvstoreHashtableFind(server.pubsubshard_channels, slot, c->argv[j], &found);
            hashtable *clients = found;
            addReplyBulk(c, c->argv[j]);
            addReplyLongLong(c, clients ? hashtableSize(clients) : 0);
        }
    } else if (!strcasecmp(objectGetVal(c->argv[1]), "conflated") && c->argc == 2) {
        /* PUBSUB CONFLATED */
        addReplyArrayLen(c, 26);
        addReplyBulkCString(c, "channels");
        addReplyLongLong(c, dictSize(server.pubsub_conflated_channels));
        addReplyBulkCString(c, "patterns");
        addReplyLongLong(c, dictSize(server.pubsub_conflated_patterns));
        addReplyBulkCString(c, "shard_channels");
        addReplyLongLong(c, kvstoreSize(server.pubsub_conflated_shard_channels));
        addReplyBulkCString(c, "subscriptions");
        addReplyLongLong(c, server.pubsub_conflated_subscriptions);
        addReplyBulkCString(c, "shard_subscriptions");
        addReplyLongLong(c, server.pubsub_conflated_shard_subscriptions);
        addReplyBulkCString(c, "pending");
        addReplyLongLong(c, server.pubsub_conflated_pending);
        addReplyBulkCString(c, "pending_bytes");
        addReplyLongLong(c, server.pubsub_conflated_pending_bytes);
        addReplyBulkCString(c, "pattern_entries");
        addReplyLongLong(c, server.pubsub_conflated_pattern_entries);
        addReplyBulkCString(c, "delivered");
        addReplyLongLong(c, server.pubsub_conflated_delivered);
        addReplyBulkCString(c, "replaced");
        addReplyLongLong(c, server.pubsub_conflated_replaced);
        addReplyBulkCString(c, "dropped");
        addReplyLongLong(c, server.pubsub_conflated_dropped);
        addReplyBulkCString(c, "evicted");
        addReplyLongLong(c, server.pubsub_conflated_evicted);
        addReplyBulkCString(c, "flush_truncated");
        addReplyLongLong(c, server.pubsub_conflated_flush_truncated);
    } else if (!strcasecmp(objectGetVal(c->argv[1]), "cnumsub") && c->argc >= 2) {
        /* PUBSUB CNUMSUB [<channel> ...] */
        int j;
        addReplyArrayLen(c, (c->argc - 2) * 2);
        for (j = 2; j < c->argc; j++) {
            dictEntry *de = dictFind(server.pubsub_conflated_channels, c->argv[j]);
            hashtable *clients = de ? dictGetVal(de) : NULL;
            addReplyBulk(c, c->argv[j]);
            addReplyLongLong(c, clients ? hashtableSize(clients) : 0);
        }
    } else if (!strcasecmp(objectGetVal(c->argv[1]), "cnumpat") && c->argc == 2) {
        /* PUBSUB CNUMPAT: sum of pattern subscriptions, not distinct patterns. */
        unsigned long long total = 0;
        dictIterator *di = dictGetIterator(server.pubsub_conflated_patterns);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL) total += hashtableSize(dictGetVal(de));
        dictReleaseIterator(di);
        addReplyLongLong(c, total);
    } else if (!strcasecmp(objectGetVal(c->argv[1]), "cchannels") && (c->argc == 2 || c->argc == 3)) {
        /* PUBSUB CCHANNELS [<pattern>] */
        sds pat = (c->argc == 2) ? NULL : objectGetVal(c->argv[2]);
        void *replylen = addReplyDeferredLen(c);
        long count = 0;
        dictIterator *di = dictGetIterator(server.pubsub_conflated_channels);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL) {
            robj *channel = dictGetKey(de);
            if (!pat || stringmatchlen(pat, sdslen(pat), objectGetVal(channel), sdslen(objectGetVal(channel)), 0)) {
                addReplyBulk(c, channel);
                count++;
            }
        }
        dictReleaseIterator(di);
        setDeferredArrayLen(c, replylen, count);
    } else if (!strcasecmp(objectGetVal(c->argv[1]), "cshardchannels") && (c->argc == 2 || c->argc == 3)) {
        /* PUBSUB CSHARDCHANNELS [<pattern>] */
        sds pat = (c->argc == 2) ? NULL : objectGetVal(c->argv[2]);
        channelList(c, pat, server.pubsub_conflated_shard_channels);
    } else if (!strcasecmp(objectGetVal(c->argv[1]), "cshardnumsub") && c->argc >= 2) {
        /* PUBSUB CSHARDNUMSUB [<shardchannel> ...] */
        int j;
        addReplyArrayLen(c, (c->argc - 2) * 2);
        for (j = 2; j < c->argc; j++) {
            sds key = objectGetVal(c->argv[j]);
            unsigned int slot = server.cluster_enabled ? keyHashSlot(key, (int)sdslen(key)) : 0;
            void *found = NULL;
            kvstoreHashtableFind(server.pubsub_conflated_shard_channels, slot, c->argv[j], &found);
            hashtable *clients = found;
            addReplyBulk(c, c->argv[j]);
            addReplyLongLong(c, clients ? hashtableSize(clients) : 0);
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

void channelList(client *c, sds pat, kvstore *pubsub_channels) {
    long mblen = 0;
    void *replylen;
    unsigned int slot_cnt = kvstoreNumHashtables(pubsub_channels);

    replylen = addReplyDeferredLen(c);
    for (unsigned int i = 0; i < slot_cnt; i++) {
        if (!kvstoreHashtableSize(pubsub_channels, i)) continue;
        kvstoreHashtableIterator *kvs_di = kvstoreGetHashtableIterator(pubsub_channels, i, 0);
        void *next;
        while (kvstoreHashtableIteratorNext(kvs_di, &next)) {
            hashtable *clients = next;
            robj *cobj = *(robj **)hashtableMetadata(clients);
            sds channel = objectGetVal(cobj);

            if (!pat || stringmatchlen(pat, sdslen(pat), channel, sdslen(channel), 0)) {
                addReplyBulk(c, cobj);
                mblen++;
            }
        }
        kvstoreReleaseHashtableIterator(kvs_di);
    }
    setDeferredArrayLen(c, replylen, mblen);
}

/* SPUBLISH <shardchannel> <message> */
void spublishCommand(client *c) {
    if (server.cluster_enabled && !clusterPublishMessageLengthValid(c->argv[1], c->argv[2])) {
        addReplyError(c, "Publish message is too large for the cluster bus");
        return;
    }
    int receivers = pubsubPublishMessageAndPropagateToCluster(c->argv[1], c->argv[2], 1);
    if (!server.cluster_enabled) forceCommandPropagation(c, PROPAGATE_REPL);
    addReplyLongLong(c, receivers);
}

/* SSUBSCRIBE shardchannel [shardchannel ...] */
void ssubscribeCommand(client *c) {
    if (c->flag.deny_blocking) {
        /* A client that has CLIENT_DENY_BLOCKING flag on
         * expect a reply per command and so cannot execute subscribe. */
        addReplyError(c, "SSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }

    for (int j = 1; j < c->argc; j++) {
        long long interval = pubsubAutoConflateInterval(c, c->argv[j]);
        if (interval > 0) {
            pubsubSubscribeConflatedShardChannel(c, c->argv[j], interval);
        } else {
            pubsubSubscribeChannel(c, c->argv[j], pubSubShardType);
        }
    }
    markClientAsPubSub(c);
}

/* SUNSUBSCRIBE [shardchannel [shardchannel ...]] */
void sunsubscribeCommand(client *c) {
    if (!c->pubsub_data) initClientPubSubData(c);

    if (c->argc == 1) {
        int removed = 0;
        hashtableIterator iter;
        hashtableInitIterator(&iter, c->pubsub_data->pubsubshard_channels, HASHTABLE_ITER_SAFE);
        void *channel;
        while (hashtableNext(&iter, &channel)) {
            removed += pubsubUnsubscribeAutoShardChannel(c, channel, 1);
        }
        hashtableCleanupIterator(&iter);

        dictIterator *di = dictGetSafeIterator(c->pubsub_data->conflated_shard_channels);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL) {
            removed += pubsubUnsubscribeAutoShardChannel(c, dictGetKey(de), 1);
        }
        dictReleaseIterator(di);
        if (removed == 0) addReplyPubsubUnsubscribed(c, NULL, pubSubShardType);
    } else {
        for (int j = 1; j < c->argc; j++) {
            pubsubUnsubscribeAutoShardChannel(c, c->argv[j], 1);
        }
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

size_t pubsubMemOverhead(client *c) {
    if (!c->pubsub_data) return 0;
    /* PubSub patterns */
    size_t mem = hashtableMemUsage(c->pubsub_data->pubsub_patterns);
    /* Global PubSub channels */
    mem += hashtableMemUsage(c->pubsub_data->pubsub_channels);
    /* Sharded PubSub channels */
    mem += hashtableMemUsage(c->pubsub_data->pubsubshard_channels);
    /* Conflated channels/patterns, including sharded subscriptions. */
    mem += hashtableMemUsage(c->pubsub_data->conflated_channels);
    mem += hashtableMemUsage(c->pubsub_data->conflated_patterns);
    mem += hashtableMemUsage(c->pubsub_data->conflated_shard_channels);
    /* Pending payload bytes are client memory and must count for
     * maxmemory-clients / CLIENT LIST tot-mem. */
    mem += c->pubsub_data->conflated_pending_bytes;
    /* Account for channel/pattern name objects. A channel name robj
     * is shared (via refcount) by all its subscribers, but we attribute
     * it to each subscribing client so it stays visible to CLIENT INFO
     * and maxmemory-clients. */
    mem += c->pubsub_data->pubsub_object_mem;
    return mem;
}

int pubsubTotalSubscriptions(void) {
    return dictSize(server.pubsub_patterns) + kvstoreSize(server.pubsub_channels) +
           kvstoreSize(server.pubsubshard_channels) + dictSize(server.pubsub_conflated_channels) +
           dictSize(server.pubsub_conflated_patterns) + kvstoreSize(server.pubsub_conflated_shard_channels);
}
