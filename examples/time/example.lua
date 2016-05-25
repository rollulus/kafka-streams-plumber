local timecvt = require "timecvt"

-- input: isotime:string, n:string
-- output: unixmilis:number, n:number if n can be parsed

return pb
	.filter(function(k,v)
			return tonumber(v) ~= nil
		end)
	.map(function(k,v)
			return math.floor(timecvt.iso8601ToUnixTimestamp(k) * 1000), tonumber(v)
		end)
