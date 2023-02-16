import pytest

import ansq

@pytest.mark.asyncio
async def test_can_write_and_read_back():
    message = b"May there be light!"
    tangled_address = ["tangled:6000"]

    writer = await ansq.create_writer(nsqd_tcp_addresses=tangled_address)
    await writer.pub( topic="example_topic", message=message)

    reader = await ansq.create_reader(
        nsqd_tcp_addresses=tangled_address,
        topic="example_topic",
        channel="example_channel",
    )
    async for message_received in reader.messages():
        assert message_received.body == message
        break
