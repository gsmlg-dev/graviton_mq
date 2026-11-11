# Under host load, asynchronous messages (such as a crash's DOWN) can exceed
# ExUnit's 100 ms default. This default applies to every assert_receive here;
# a missing message still fails, and refute_receive keeps its own default.
ExUnit.start(assert_receive_timeout: 2_000)
