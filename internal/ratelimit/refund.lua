-- Give back unused send slots (e.g. we took 5 but only 2 rows were due) by
-- removing that many of the newest grants from the window.
--
-- KEYS[1] window key, ARGV[1] slots to return

return #redis.call('ZPOPMAX', KEYS[1], tonumber(ARGV[1])) / 2
