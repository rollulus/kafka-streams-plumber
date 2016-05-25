local someInputs = {
	pb.keyValue("2016-05-13T16:43:12.345+00:00", "42"),
	pb.keyValue("2014-11-24T23:16:42.345+01:00", "three"), -- gets filtered out
	pb.keyValue("2016-05-13T16:43:13.37Z", "37")
}

local whatWeExpect = {
	pb.keyValue(1463157792345, 42),
	pb.keyValue(1463157793370, 37)
}

return pb
	.forInputs(someInputs)
	.expectOutputs(whatWeExpect)
