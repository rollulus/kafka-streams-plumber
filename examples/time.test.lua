local someInputs = {
	pb.value{
		date = "2016-05-13T16:43:12.345+00:00",
	}, 
	pb.value{
		date = "2016-05-13T16:43:13.37Z",
	},
}

local whatWeExpect = {
	pb.value{
		timestamp = 1463157792.345,
	},
	pb.value{
		timestamp = 1463157793.37,
	},
}

return pb
	.forInputs(someInputs)
	.expectOutputs(whatWeExpect)
