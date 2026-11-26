# Conflated Pub/Sub tests. C* commands are an extension of this fork.

start_server {tags {"pubsub"}} {
    test "CSUBSCRIBE delivers only the latest value per interval" {
        r config set conflated-pubsub-interval-ms 100
        set rd [valkey_deferring_client]
        $rd CSUBSCRIBE cfoo
        assert_equal {subscribe cfoo 1} [$rd read]
        # PUBLISH counts the subscriber that accepted the value, even though
        # delivery is deferred to the interval tick.
        assert_equal 1 [r publish cfoo 1]
        assert_equal 1 [r publish cfoo 2]
        after 300
        assert_equal {message cfoo 2} [$rd read]
        $rd close
    }

    test "CPSUBSCRIBE keeps one latest value per matching channel" {
        set rd [valkey_deferring_client]
        $rd CPSUBSCRIBE cbar:*
        assert_equal {psubscribe cbar:* 1} [$rd read]
        assert_equal 1 [r publish cbar:a 1]
        assert_equal 1 [r publish cbar:a 2]
        assert_equal 1 [r publish cbar:b 3]
        after 300
        set first [$rd read]
        set second [$rd read]
        assert_equal {pmessage cbar:* cbar:a 2} $first
        assert_equal {pmessage cbar:* cbar:b 3} $second
        $rd close
    }

    test "conflated-pubsub-auto rules route and set the interval" {
        r config set conflated-pubsub-interval-ms 200
        r acl setuser ruleuser on nopass resetchannels &* +@pubsub
        r config set conflated-pubsub-auto "ruleuser irule:* 30"
        set rd [valkey_deferring_client]
        $rd AUTH ruleuser x
        assert_equal OK [$rd read]
        $rd SUBSCRIBE irule:x
        assert_equal {subscribe irule:x 1} [$rd read]
        # The rule routed it to the conflated index, not the stock one.
        assert_equal {irule:x 1} [r pubsub cnumsub irule:x]
        r publish irule:x one
        after 200
        assert_equal {message irule:x one} [$rd read]
        $rd close
        r config set conflated-pubsub-auto ""
        r acl deluser ruleuser
    }

    test "PUBSUB CONFLATED and CNUMSUB expose conflated subscriptions" {
        set rd [valkey_deferring_client]
        $rd CSUBSCRIBE cstats
        assert_equal {subscribe cstats 1} [$rd read]
        assert_equal {cstats 1} [r pubsub cnumsub cstats]
        set stats [dict create {*}[r pubsub conflated]]
        assert_equal 1 [dict get $stats channels]
        assert_equal 1 [dict get $stats subscriptions]
        assert {[dict get $stats pending] >= 0}
        $rd close
    }

    test "conflated pending memory caps drop and recycle entries" {
        r config set conflated-pubsub-max-pending-keys 2
        r config set conflated-pubsub-max-pending-bytes 1024
        set rd [valkey_deferring_client]
        $rd CPSUBSCRIBE ccap:*
        assert_equal {psubscribe ccap:* 1} [$rd read]
        set payload [string repeat z 512]
        for {set i 0} {$i < 5} {incr i} {
            r publish ccap:$i $payload
        }
        after 300
        set stats [dict create {*}[r pubsub conflated]]
        assert {[dict get $stats dropped] > 0}
        assert {[dict get $stats pattern_entries] <= 2}
        assert {[dict get $stats pending_bytes] <= 1024}
        $rd close
        r config set conflated-pubsub-max-pending-keys 100000
        r config set conflated-pubsub-max-pending-bytes 67108864
        r config set conflated-pubsub-interval-ms 0
    }

    test "CLIENT LIST exposes conflated subscription counts" {
        r config set conflated-pubsub-interval-ms 100
        set rd [valkey_deferring_client]
        $rd CSUBSCRIBE clist:exact
        assert_equal {subscribe clist:exact 1} [$rd read]
        $rd CPSUBSCRIBE clist:*
        assert_equal {psubscribe clist:* 2} [$rd read]
        $rd CSSUBSCRIBE clist:shard
        assert_equal {ssubscribe clist:shard 1} [$rd read]
        set info [r client list]
        assert_match {*csub=1*} $info
        assert_match {*cpsub=1*} $info
        assert_match {*cssub=1*} $info
        $rd close
        set info [r client list]
        assert {![string match {*csub=1*} $info]}
        r config set conflated-pubsub-interval-ms 0
    }

    test "CSSUBSCRIBE delivers only the latest shard value" {
        r config set conflated-pubsub-interval-ms 100
        set rd [valkey_deferring_client]
        $rd CSSUBSCRIBE cshard
        assert_equal {ssubscribe cshard 1} [$rd read]
        assert_equal 1 [r spublish cshard 1]
        assert_equal 1 [r spublish cshard 2]
        assert_equal {cshard 1} [r pubsub cshardnumsub cshard]
        assert_equal {cshard} [r pubsub cshardchannels csh*]
        set stats [dict create {*}[r pubsub conflated]]
        assert_equal 1 [dict get $stats shard_channels]
        assert_equal 1 [dict get $stats shard_subscriptions]
        after 300
        assert_equal {smessage cshard 2} [$rd read]
        $rd CSUNSUBSCRIBE cshard
        assert_equal {sunsubscribe cshard 0} [$rd read]
        $rd close
        r config set conflated-pubsub-interval-ms 0
    }

    test "SSUBSCRIBE auto rule routes to the conflated shard index" {
        r config set conflated-pubsub-interval-ms 200
        r acl setuser sharduser on nopass resetchannels &* +@pubsub
        r config set conflated-pubsub-auto "sharduser ishard:* 30"
        set rd [valkey_deferring_client]
        $rd AUTH sharduser x
        assert_equal OK [$rd read]
        $rd SSUBSCRIBE ishard:x
        assert_equal {ssubscribe ishard:x 1} [$rd read]
        assert_equal {ishard:x 1} [r pubsub cshardnumsub ishard:x]
        r spublish ishard:x one
        after 200
        assert_equal {smessage ishard:x one} [$rd read]
        $rd close
        r config set conflated-pubsub-auto ""
        r acl deluser sharduser
        r config set conflated-pubsub-interval-ms 0
    }

    test "CONFIG REWRITE persists ordered auto rules" {
        r config set conflated-pubsub-interval-ms 100
        r config set conflated-pubsub-auto "app tick:* 50 feed * 200"
        r config rewrite
        set config_file [srv 0 config_file]
        set f [open $config_file r]
        set content [read $f]
        close $f
        set count 0
        foreach line [split $content "\n"] {
            if {[string match "conflated-pubsub-auto *" $line]} { incr count }
        }
        assert_equal 2 $count
        restart_server 0 true false
        set rules [lindex [r config get conflated-pubsub-auto] 1]
        assert_match {*tick:*" 50*} $rules
        assert_match {*feed*200*} $rules
        r config set conflated-pubsub-auto ""
        r config set conflated-pubsub-interval-ms 0
    }

    test "auto rules support spaces, colons and equals" {
        r config set conflated-pubsub-interval-ms 100
        r config set conflated-pubsub-auto {* "space chan:eq=1" 50}
        r config rewrite
        set f [open [srv 0 config_file] r]
        set content [read $f]
        close $f
        assert_match {*"space chan:eq=1"*} $content

        restart_server 0 true false
        set rd [valkey_deferring_client]
        $rd SUBSCRIBE {space chan:eq=1}
        assert_equal [list subscribe "space chan:eq=1" 1] [$rd read]
        assert_equal [list "space chan:eq=1" 1] [r pubsub cnumsub {space chan:eq=1}]
        r publish {space chan:eq=1} value
        after 200
        assert_equal [list message "space chan:eq=1" value] [$rd read]
        $rd close
        r config set conflated-pubsub-auto ""
        r config set conflated-pubsub-interval-ms 0
    }

    test "ACL revocation closes conflated shard subscribers" {
        r config set conflated-pubsub-interval-ms 100
        r acl setuser shardrevoke on nopass resetchannels &ok:* +@pubsub
        set rd [valkey_deferring_client]
        $rd AUTH shardrevoke x
        assert_equal OK [$rd read]
        $rd CSSUBSCRIBE ok:shard
        assert_equal {ssubscribe ok:shard 1} [$rd read]
        r acl setuser shardrevoke resetchannels &other:* +@pubsub
        after 100
        set stats [dict create {*}[r pubsub conflated]]
        assert_equal 0 [dict get $stats shard_subscriptions]
        set info [r client list]
        assert {![string match {*cssub=1*} $info]}
        $rd close
        r acl deluser shardrevoke
        r config set conflated-pubsub-interval-ms 0
    }

    test "conflated subscriptions survive active defrag" {
        r config set conflated-pubsub-interval-ms 1000
        r config set activedefrag yes
        r config set active-defrag-threshold-lower 0
        r config set active-defrag-ignore-bytes 1
        set rd [valkey_deferring_client]
        set channels {}
        for {set i 0} {$i < 50} {incr i} { lappend channels "defrag:$i" }
        $rd CSUBSCRIBE {*}$channels
        for {set i 0} {$i < 50} {incr i} { $rd read }
        for {set round 0} {$round < 10} {incr round} {
            foreach channel $channels {
                r publish $channel "v$round"
            }
        }
        after 1500
        array set latest {}
        for {set i 0} {$i < 50} {incr i} {
            lassign [$rd read] _ channel payload
            set latest($channel) $payload
        }
        foreach channel $channels {
            assert_equal v9 $latest($channel)
        }
        $rd close
        r config set activedefrag no
        r config set active-defrag-threshold-lower 10
        r config set active-defrag-ignore-bytes 100mb
        r config set conflated-pubsub-interval-ms 0
    }

    test "changing the interval reschedules pending values" {
        r config set conflated-pubsub-interval-ms 1000
        set rd [valkey_deferring_client]
        $rd CSUBSCRIBE slow:interval
        assert_equal {subscribe slow:interval 1} [$rd read]
        r publish slow:interval value
        r config set conflated-pubsub-interval-ms 50
        after 600
        assert_equal {message slow:interval value} [$rd read]
        $rd close
        r config set conflated-pubsub-interval-ms 0
    }

    test "conflated shard subscriptions survive active defrag" {
        r config set conflated-pubsub-interval-ms 1000
        r config set activedefrag yes
        r config set active-defrag-threshold-lower 0
        r config set active-defrag-ignore-bytes 1
        set rd [valkey_deferring_client]
        $rd CSSUBSCRIBE defrag:shard
        assert_equal {ssubscribe defrag:shard 1} [$rd read]
        for {set i 0} {$i < 20} {incr i} { r spublish defrag:shard "s$i" }
        after 1500
        assert_equal {smessage defrag:shard s19} [$rd read]
        $rd close
        r config set activedefrag no
        r config set active-defrag-threshold-lower 10
        r config set active-defrag-ignore-bytes 100mb
        r config set conflated-pubsub-interval-ms 0
    }

    test "conflated delivery keeps empty, binary and integer payloads" {
        r config set conflated-pubsub-interval-ms 100
        set rd [valkey_deferring_client]
        $rd CSUBSCRIBE cpayload
        assert_equal {subscribe cpayload 1} [$rd read]
        r publish cpayload ""
        after 250
        assert_equal [list message cpayload ""] [$rd read]
        set binary_payload [binary format H* "00ff01"]
        r publish cpayload $binary_payload
        after 250
        assert_equal [list message cpayload $binary_payload] [$rd read]
        r publish cpayload 12345
        after 250
        assert_equal [list message cpayload 12345] [$rd read]
        $rd close
        r config set conflated-pubsub-interval-ms 0
    }
}
