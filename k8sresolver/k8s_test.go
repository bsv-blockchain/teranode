package k8sresolver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestResolve(t *testing.T) {
	s := serviceClient{
		namespace: "default",
		k8s: fake.NewSimpleClientset(
			&v1.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test",
					Namespace:   "default",
					Annotations: map[string]string{},
				},
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{IP: "1.1.1.1"},
						},
					},
				}},
		),
	}

	res, err := s.Resolve(context.Background(), "test", "0")
	if assert.NoError(t, err) {
		assert.Equal(t, []string{"1.1.1.1:0"}, res)
	}
}

func TestWatch(t *testing.T) {
	s := serviceClient{
		namespace: "default",
		k8s: fake.NewSimpleClientset(
			&v1.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test",
					Namespace:   "default",
					Annotations: map[string]string{},
				},
				Subsets: []v1.EndpointSubset{
					{
						Addresses: []v1.EndpointAddress{
							{IP: "1.1.1.1"},
						},
					},
				}},
		),
	}

	_, _, err := s.Watch(context.Background(), "test")
	assert.NoError(t, err)
}
