import asyncio
from behave import when
from behave.api.async_step import async_run_until_complete as async_step

@when('after a pause of {duration} seconds')
@async_step
async def step_impl(context, duration):
    await asyncio.sleep(float(duration))