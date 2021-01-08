package eventbus

import "testing"

func TestMatchTopicExact(t *testing.T) {
	if !MatchTopic("orders.created", "orders.created") {
		t.Fatal("expected exact match to succeed")
	}
	if MatchTopic("orders.created", "orders.shipped") {
		t.Fatal("expected exact mismatch to fail")
	}
}

func TestMatchTopicWildcardSegment(t *testing.T) {
	cases := []struct {
		pattern string
		topic   string
		want    bool
	}{
		{"orders.*", "orders.created", true},
		{"orders.*", "orders.shipped", true},
		{"orders.*", "orders", false},
		{"orders.*", "orders.created.v2", false},
		{"*.created", "orders.created", true},
		{"*.created", "users.created", true},
		{"*.created", "orders.shipped", false},
		{"*", "orders", true},
		{"*", "orders.created", false},
	}

	for _, c := range cases {
		if got := MatchTopic(c.pattern, c.topic); got != c.want {
			t.Errorf("MatchTopic(%q, %q) = %v, want %v", c.pattern, c.topic, got, c.want)
		}
	}
}

func TestMatchTopicMultipleWildcards(t *testing.T) {
	if !MatchTopic("*.*.created", "orders.us.created") {
		t.Fatal("expected multi-wildcard pattern to match")
	}
	if MatchTopic("*.*.created", "orders.created") {
		t.Fatal("expected multi-wildcard pattern to require matching segment count")
	}
}
