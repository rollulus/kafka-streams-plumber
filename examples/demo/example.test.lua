local someInputs = {
	pb.value{
		redundantField = 7,
		notValid=true,
		person = { name = "roEl", species = "human"},
		fingers_lh = 7,
		fingers_rh = 7,
	}, pb.value{
		redundantField = 127,
		notValid=false,
		person = { name = "ROELLL", species = "homo sapiens"},
		fingers_lh = 1,
		fingers_rh = 3,
	}
}

local whatWeExpect = {
	pb.value{
		valid = false,
		name = "roel",
		fingers = 14,
	},pb.value{
		valid = true,
		name = "roelll",
		fingers = 4,
	}
}

return pb
	.forInputs(someInputs)
	.expectOutputs(whatWeExpect)
