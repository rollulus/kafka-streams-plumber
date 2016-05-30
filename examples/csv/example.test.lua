local someInputs = {
    pb.value("fail,2012-09-03T11:33:46Z,2"),            -- unhappy path #1
    pb.value("dudebowski,2012-09-03T11:33:46Z,,2"),     -- no `blog`
    pb.value(""),                                       -- unhappy path #2
    pb.value("rollulus,2015-01-14T07:36:24Z,https://keybase.io/rollulus,8"),
    pb.value("error"),                                  -- unhappy path #3
}

local whatWeExpect = {
    pb.value{
        login="dudebowski",
        created_at="2012-09-03T11:33:46Z",
        blog=nil,
        public_repos=2
    },
    pb.value{
        login="rollulus",
        created_at="2015-01-14T07:36:24Z",
        blog="https://keybase.io/rollulus",
        public_repos=8
    },
}

return pb
    .forInputs(someInputs)
    .expectOutputs(whatWeExpect)
