package auth

import "testing"

func TestCheckKey(t *testing.T) {
	cases := []struct {
		name      string
		expected  string
		presented []string
		want      error
	}{
		{"disabled", "", nil, nil},
		{"disabled with key", "", []string{"x"}, nil},
		{"missing", "secret", nil, ErrMissingKey},
		{"invalid", "secret", []string{"nope"}, ErrInvalidKey},
		{"valid", "secret", []string{"secret"}, nil},
		{"valid among many", "secret", []string{"a", "secret"}, nil},
		{"prefix is not enough", "secret", []string{"secre"}, ErrInvalidKey},
	}
	for _, c := range cases {
		if got := CheckKey(c.expected, c.presented); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
