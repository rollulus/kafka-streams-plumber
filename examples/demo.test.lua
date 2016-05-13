return pb.forInputs{pb.value{
		notValid=true,
		person = { name = "roEl"},
		fingers_lh = 7,
		fingers_rh = 7,
	},pb.value{
		notValid=false,
		person = { name = "ROELLL"},
		fingers_lh = 2,
		fingers_rh = 2,
	}}
	.expectOutputs{pb.value{
		valid = false,
		name = "roel",
		fingers = 14,
	},pb.value{
		valid = true,
		name = "roelll",
		fingers = 4,
	}}