from src.data_nodes.example_nodes import DailyRandomNumber, RandomDataNodeConfig, VolatilityConfig


daily_node = DailyRandomNumber(node_configuration=RandomDataNodeConfig(mean=0.0))
# only enterprise clients can run debug_mode ==False, so lets keep it True here
# force_update = True indicates that the node should not wait for its internal schedule
daily_node.run(debug_mode=True, force_update=True)



daily_node = DailyRandomNumber(node_configuration=RandomDataNodeConfig(mean=0.0, std=VolatilityConfig(center=2.0, skew=False)))
# only enterprise clients can run debug_mode ==False, so lets keep it True here
# force_update = True indicates that the node should not wait for its internal schedule
daily_node.run(debug_mode=True, force_update=True)