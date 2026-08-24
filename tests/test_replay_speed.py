from sniper_bot.replay import ReplaySpeed


def test_replay_speed_wall_time_divisors_and_legacy_aliases() -> None:
    assert ReplaySpeed.ONE_X.wall_time_divisor == 1.0
    assert ReplaySpeed.REALTIME.wall_time_divisor == 1.0
    assert ReplaySpeed.FIVE_X.wall_time_divisor == 5.0
    assert ReplaySpeed.TEN_X.wall_time_divisor == 10.0
    assert ReplaySpeed.MAXIMUM.wall_time_divisor is None
    assert ReplaySpeed.MAX.wall_time_divisor is None
