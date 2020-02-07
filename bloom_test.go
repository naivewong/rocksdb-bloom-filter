package bloom

import (
	"fmt"
	"path/filepath"
	"runtime"
	"reflect"
	"testing"
)

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}

func TestBloom(t *testing.T) {
	p := NewBloomFilterPolicy(10)
	b := p.GetFilterBitsBuilder()
	for i := 0; i < 10; i++ {
		b.AddKey([]byte(fmt.Sprintf("key%d", i)))
	}
	content := b.Finish()

	r := p.GetFilterBitsReader(content)
	for i := 0; i < 10; i++ {
		equals(t, true, r.KeyMayMatch([]byte(fmt.Sprintf("key%d", i))))
	}
	for i := 10; i < 20; i++ {
		equals(t, false, r.KeyMayMatch([]byte(fmt.Sprintf("key%d", i))))
	}
}