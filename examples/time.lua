local timecvt = require "timecvt"

return pb.mapValues(function(i)
	return {
		timestamp = timecvt.iso8601ToUnixTimestamp(i.date),
	}
end)

