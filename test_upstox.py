import asyncio
import aiohttp
from algo_trader import UpstoxClient

async def test():
    async with aiohttp.ClientSession() as session:
        url = "/historical-candle/intraday/NSE_FO|51714/1minute"
        res = await UpstoxClient.get(session, url)
        if res and "candles" in res:
            print("SUCCESS! Volume=", res["candles"][-1][5])
        else:
            print("FAILED:", res)

asyncio.run(test())
