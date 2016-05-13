return pb.mapValues(function(undesired)
	return {
		valid = not undesired.notValid,
		name = undesired.person.name:lower(),
		fingers = undesired.fingers_lh + undesired.fingers_rh
	}
end)

