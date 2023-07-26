package k8sresolver

// func _TestResolve(t *testing.T) {
// 	s := serviceClient{
// 		namespace: "default",
// 		k8s: fake.NewSimpleClientset(
// 			&v1.Endpoints{
// 				ObjectMeta: metav1.ObjectMeta{
// 					Name:        "test",
// 					Namespace:   "default",
// 					Annotations: map[string]string{},
// 				},
// 				Subsets: []v1.EndpointSubset{
// 					{
// 						Addresses: []v1.EndpointAddress{
// 							{IP: "1.1.1.1"},
// 						},
// 					},
// 				}},
// 		),
// 		logger: gocore.Log("k8sresolver", gocore.NewLogLevelFromString("PANIC")),
// 	}

// 	res, err := s.Resolve(context.Background(), "test", "0")
// 	if assert.NoError(t, err) {
// 		assert.Equal(t, []string{"1.1.1.1:0"}, res)
// 	}
// }

// func _TestWatch(t *testing.T) {
// 	s := serviceClient{
// 		namespace: "default",
// 		k8s: fake.NewSimpleClientset(
// 			&v1.Endpoints{
// 				ObjectMeta: metav1.ObjectMeta{
// 					Name:        "test",
// 					Namespace:   "default",
// 					Annotations: map[string]string{},
// 				},
// 				Subsets: []v1.EndpointSubset{
// 					{
// 						Addresses: []v1.EndpointAddress{
// 							{IP: "1.1.1.1"},
// 						},
// 					},
// 				}},
// 		),
// 		logger: gocore.Log("k8sresolver", gocore.NewLogLevelFromString("PANIC")),
// 	}

// 	_, _, err := s.Watch(context.Background(), "test")
// 	assert.NoError(t, err)
// }
