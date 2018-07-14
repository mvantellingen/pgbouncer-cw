package main

func stringPtr(input string) *string {
	return &input
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
