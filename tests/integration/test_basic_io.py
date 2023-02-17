import uuid

import ansq
import faker
import pytest

fake = faker.Faker()


@pytest.mark.asyncio
async def test_write_many_messages_to_multiple_topics():
    tangled_address = ["tangled:6000"]
    writer = await ansq.create_writer(nsqd_tcp_addresses=tangled_address)

    data = [
        (uuid.uuid4().hex, [fake.binary(length=200) for message_count in range(20)])
        for topic_count in range(5)
    ]

    for topic_name, messages in data:
        for message in messages:
            await writer.pub(topic=topic_name, message=message)

    await writer.close()

    for topic_name, messages in data:
        reader = await ansq.create_reader(
            nsqd_tcp_addresses=tangled_address,
            topic=topic_name,
            channel=uuid.uuid4().hex,
        )
        for message_expected in messages:
            message_actual = await anext(reader.messages())
            await message_actual.fin()
            assert message_actual.body == message_expected

        await reader.close()
