return pb
    .mapValues(
        function(csvLine)       -- line:string to an array with columns
            return table.pack(csvLine:match("^(%P+),([^,]+),([^,]*),(%d+)$"))
        end)
    .filter(
        function(key, columns)  -- filter out incomplete records
            return #columns == 4
        end)
    .mapValues(
        function(columns)       -- column array to structure
            local col3 = columns[3]
            if col3 == "" then col3 = nil end
            return {
                login = columns[1],
                created_at = columns[2],
                blog = col3,
                public_repos = tonumber(columns[4])
            }
        end)
