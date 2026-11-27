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

    proc conflated_client_field {name field} {
        set clients [split [string trim [r client list]] "\r\n"]
        set line [lsearch -inline $clients *name=$name*]
        if {$line eq ""} { error "client $name not found" }
        if {![regexp "$field=(\[0-9\]+)" $line - value]} {
            error "field $field not found for $name"
        }
        return $value
    }

    proc conflated_stat {field} {
        return [dict get [dict create {*}[r pubsub conflated]] $field]
    }

    test "conflated memory is attributed to CLIENT LIST tot-mem" {
        r config set conflated-pubsub-interval-ms 60000
        set rd [valkey_deferring_client]
        $rd client setname mem-client
        assert_equal OK [$rd read]
        set base [conflated_client_field mem-client tot-mem]

        set channels {}
        for {set i 0} {$i < 100} {incr i} {
            lappend channels "mem:exact:$i:0123456789abcdef"
        }
        $rd CSUBSCRIBE {*}$channels
        for {set i 0} {$i < 100} {incr i} { $rd read }
        set exact_mem [conflated_client_field mem-client tot-mem]
        # Every channel name is 28 bytes; the struct/table overhead adds more.
        assert {$exact_mem - $base >= 100 * 28}

        $rd CUNSUBSCRIBE {*}$channels
        for {set i 0} {$i < 100} {incr i} { $rd read }
        set after_exact [conflated_client_field mem-client tot-mem]
        assert {$after_exact <= $base + 32768}

        $rd CPSUBSCRIBE mempat:*
        assert_equal {psubscribe mempat:* 1} [$rd read]
        for {set i 0} {$i < 100} {incr i} {
            r publish "mempat:$i:0123456789abcdef" value
        }
        set pattern_mem [conflated_client_field mem-client tot-mem]
        assert {$pattern_mem - $after_exact >= 100 * 28}
        $rd CPUNSUBSCRIBE mempat:*
        assert_match {punsubscribe mempat:* *} [$rd read]
        set after_pattern [conflated_client_field mem-client tot-mem]
        assert {$after_pattern <= $base + 32768}

        set shard_channels {}
        for {set i 0} {$i < 100} {incr i} {
            lappend shard_channels "mem:shard:$i:0123456789abcdef"
        }
        $rd CSSUBSCRIBE {*}$shard_channels
        for {set i 0} {$i < 100} {incr i} { $rd read }
        set shard_mem [conflated_client_field mem-client tot-mem]
        assert {$shard_mem - $after_pattern >= 100 * 28}
        $rd CSUNSUBSCRIBE {*}$shard_channels
        for {set i 0} {$i < 100} {incr i} { $rd read }
        set after_shard [conflated_client_field mem-client tot-mem]
        assert {$after_shard <= $base + 65536}

        $rd close
        r config set conflated-pubsub-interval-ms 0
    }

    test "interval 0 drops pending payloads and keeps subscriptions" {
        r config set conflated-pubsub-interval-ms 500
        set rd [valkey_deferring_client]
        $rd CSUBSCRIBE itest
        assert_equal {subscribe itest 1} [$rd read]
        r publish itest value
        assert {[conflated_stat pending] >= 1}
        assert {[conflated_stat pending_bytes] >= 5}

        r config set conflated-pubsub-interval-ms 0
        assert_equal 0 [conflated_stat pending]
        assert_equal 0 [conflated_stat pending_bytes]
        assert_equal {itest 1} [r pubsub cnumsub itest]

        # Re-subscribing captures the new interval; the old payload is gone.
        r config set conflated-pubsub-interval-ms 50
        $rd CUNSUBSCRIBE itest
        assert_equal {unsubscribe itest 0} [$rd read]
        $rd CSUBSCRIBE itest
        assert_equal {subscribe itest 1} [$rd read]
        r publish itest new
        after 250
        assert_equal {message itest new} [$rd read]
        $rd close
        r config set conflated-pubsub-interval-ms 0
    }

    test "byte caps count payload bytes and not slot overhead" {
        r config set conflated-pubsub-interval-ms 60000
        r config set conflated-pubsub-max-pending-bytes 100
        r config set conflated-pubsub-max-total-pending-bytes 0
        r config set conflated-pubsub-max-pending-keys 0
        r config set conflated-pubsub-max-total-pending-keys 0
        set dropped0 [conflated_stat dropped]
        set rd [valkey_deferring_client]
        $rd CPSUBSCRIBE tiny:*
        assert_equal {psubscribe tiny:* 1} [$rd read]
        for {set i 0} {$i < 100} {incr i} {
            r publish "tiny:$i" x
        }
        assert_equal 100 [conflated_stat pending_bytes]
        assert_equal 100 [conflated_stat pattern_entries]
        r publish tiny:extra x
        assert_equal 100 [conflated_stat pending_bytes]
        assert {[conflated_stat dropped] - $dropped0 >= 1}
        $rd close
        r config set conflated-pubsub-max-pending-bytes 67108864
        r config set conflated-pubsub-max-total-pending-bytes 268435456
        r config set conflated-pubsub-max-pending-keys 100000
        r config set conflated-pubsub-max-total-pending-keys 1000000
        r config set conflated-pubsub-interval-ms 0
    }

    test "single-line auto lists trim spaces and keep literal = aliases" {
        r config set conflated-pubsub-auto ""
        r config set conflated-pubsub-auto-users ""
        r config set conflated-pubsub-auto-patterns "lit=0,inv=abc"
        r config set conflated-pubsub-interval-ms 100
        set rd [valkey_deferring_client]

        $rd SUBSCRIBE lit
        assert_equal {subscribe lit 1} [$rd read]
        assert_equal {lit 0} [r pubsub cnumsub lit]
        $rd SUBSCRIBE lit=0
        assert_equal {subscribe lit=0 2} [$rd read]
        assert_equal {lit=0 1} [r pubsub cnumsub lit=0]
        $rd SUBSCRIBE inv
        assert_equal {subscribe inv 3} [$rd read]
        assert_equal {inv 0} [r pubsub cnumsub inv]
        $rd SUBSCRIBE inv=abc
        assert_equal {subscribe inv=abc 4} [$rd read]
        assert_equal {inv=abc 1} [r pubsub cnumsub inv=abc]
        $rd close

        r config set conflated-pubsub-auto-patterns ""
        r acl setuser spaceuser on nopass resetchannels &* +@pubsub
        r config set conflated-pubsub-auto-users " spaceuser=50 "
        set rd [valkey_deferring_client]
        $rd AUTH spaceuser x
        assert_equal OK [$rd read]
        $rd SUBSCRIBE spaced
        assert_equal {subscribe spaced 1} [$rd read]
        assert_equal {spaced 1} [r pubsub cnumsub spaced]
        $rd close
        r acl deluser spaceuser
        r config set conflated-pubsub-auto-users ""
        r config set conflated-pubsub-auto-patterns ""
        r config set conflated-pubsub-interval-ms 0
    }

    test "overflow at 20k pattern entries is constant-time and bounded" {
        wait_for_condition 50 100 {
            [conflated_stat pending] == 0 && [conflated_stat pattern_entries] == 0
        } else {
            fail "pending state did not drain"
        }
        r config set conflated-pubsub-interval-ms 60000
        r config set conflated-pubsub-max-pending-keys 20000
        r config set conflated-pubsub-max-total-pending-keys 0
        r config set conflated-pubsub-max-pending-bytes 0
        r config set conflated-pubsub-max-total-pending-bytes 0
        r config set conflated-pubsub-overflow-policy drop_newest
        set dropped0 [conflated_stat dropped]
        set evicted0 [conflated_stat evicted]
        set rd [valkey_deferring_client]
        $rd CPSUBSCRIBE ovf:*
        assert_equal {psubscribe ovf:* 1} [$rd read]

        set start [clock microseconds]
        for {set i 0} {$i < 20000} {incr i} { r publish "ovf:a:$i" x }
        set fill_us [expr {[clock microseconds] - $start}]
        assert_equal 20000 [conflated_stat pattern_entries]
        assert_equal 20000 [conflated_stat pending]

        set start [clock microseconds]
        for {set i 0} {$i < 20000} {incr i} { r publish "ovf:b:$i" x }
        set drop_us [expr {[clock microseconds] - $start}]
        assert {[conflated_stat dropped] - $dropped0 >= 20000}
        assert_equal 20000 [conflated_stat pattern_entries]
        assert_equal 20000 [conflated_stat pending]
        # A drop must not scan the 20k pending index; it should stay in the
        # same order of magnitude as the fill that created it.
        assert {$drop_us < $fill_us * 4 + 2000000}

        r config set conflated-pubsub-overflow-policy drop_oldest
        set start [clock microseconds]
        for {set i 0} {$i < 20000} {incr i} { r publish "ovf:c:$i" x }
        set evict_us [expr {[clock microseconds] - $start}]
        assert {[conflated_stat evicted] - $evicted0 >= 20000}
        assert_equal 20000 [conflated_stat pattern_entries]
        assert_equal 20000 [conflated_stat pending]
        assert {$evict_us < $fill_us * 4 + 2000000}

        $rd close
        r config set conflated-pubsub-overflow-policy drop_newest
        r config set conflated-pubsub-max-pending-keys 100000
        r config set conflated-pubsub-max-total-pending-keys 1000000
        r config set conflated-pubsub-max-pending-bytes 67108864
        r config set conflated-pubsub-max-total-pending-bytes 268435456
        r config set conflated-pubsub-interval-ms 0
    }

    test "rescheduled pending values are delivered from the global queue" {
        r config set conflated-pubsub-interval-ms 60000
        set rd [valkey_deferring_client]
        $rd CPSUBSCRIBE batch:*
        assert_equal {psubscribe batch:* 1} [$rd read]
        for {set i 0} {$i < 5000} {incr i} { r publish "batch:$i" "v$i" }
        assert_equal 5000 [conflated_stat pending]
        r config set conflated-pubsub-interval-ms 50
        after 500
        array set got {}
        for {set i 0} {$i < 5000} {incr i} {
            lassign [$rd read] _ pattern channel payload
            assert_equal batch:* $pattern
            set got($channel) $payload
        }
        assert_equal 5000 [array size got]
        assert_equal v0 $got(batch:0)
        assert_equal v4999 $got(batch:4999)
        assert_equal 0 [conflated_stat pending]
        $rd close
        r config set conflated-pubsub-interval-ms 0
    }
}
