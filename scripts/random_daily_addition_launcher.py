from src.data_nodes.example_nodes import DailyRandomAddition


daily_node = DailyRandomAddition(mean=0.0, std=1.0)
daily_node.run(debug_mode=True, force_update=True)