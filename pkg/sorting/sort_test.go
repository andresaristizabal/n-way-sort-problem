package sorting

import (
	"n-way-sort/pkg/utils"
	"reflect"
	"testing"
)

func TestFileHeap_Len(t *testing.T) {
	tests := []struct {
		name string
		f    FileHeap
		want int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.f.Len(); got != tt.want {
				t.Errorf("Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileHeap_Less(t *testing.T) {
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name string
		f    FileHeap
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.f.Less(tt.args.i, tt.args.j); got != tt.want {
				t.Errorf("Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileHeap_Pop(t *testing.T) {
	tests := []struct {
		name string
		f    FileHeap
		want any
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.f.Pop(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Pop() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileHeap_Push(t *testing.T) {
	type args struct {
		x any
	}
	tests := []struct {
		name string
		f    FileHeap
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.f.Push(tt.args.x)
		})
	}
}

func TestFileHeap_Swap(t *testing.T) {
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name string
		f    FileHeap
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.f.Swap(tt.args.i, tt.args.j)
		})
	}
}

func TestSort(t *testing.T) {
	type args struct {
		config utils.Config
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Sort(tt.args.config)
		})
	}
}
