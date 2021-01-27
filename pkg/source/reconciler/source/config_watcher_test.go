package source

import "testing"

func TestKafkaConfigToJSON(t *testing.T) {

	testCases := []struct {
		name     string
		cfg      KafkaConfig
		success  bool
		expected string
	}{{
		name: "simple marshal",
		cfg: KafkaConfig{
			SaramaYamlString: `foo: bar`,
		},
		success:  true,
		expected: `{"SaramaYamlString":"foo: bar"}`,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := kafkaConfigToJSON(&tc.cfg)
			if tc.success == true && err != nil {
				t.Errorf("KafkaConfig should've been converted to JSON successfully. Err: %e", err)
			}
			if tc.success == false && err != nil {
				t.Errorf("KafkaConfig should not have been converted to JSON successfully.")
			}
			if output != tc.expected {
				t.Errorf("KafkaConfig conversion is wrong, want: %s vs got %s", tc.expected, output)
			}
		})
	}

}
