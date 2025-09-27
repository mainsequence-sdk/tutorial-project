import mainsequence.client as msc
from src.data_nodes.prices_nodes import SimulatedPrices, PriceSimulConfig


assets = msc.Asset.filter(ticker__in=["NVDA", "APPL"], )
config=PriceSimulConfig(asset_list=assets,)

batch_2_assets = msc.Asset.filter(ticker__in=["JPM", "GS"], )
config_2 = PriceSimulConfig(asset_list=batch_2_assets, )

ts = SimulatedPrices(simulation_config=config)
ts.run(debug_mode=True,force_update=True)


ts_2 = SimulatedPrices(simulation_config=config_2)
ts_2.run(debug_mode=True,force_update=True)