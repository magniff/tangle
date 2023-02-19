import uuid

import ansq
import faker
import pytest

fake = faker.Faker()


@pytest.mark.asyncio
async def test_write_many_messages_to_multiple_topics():
    tangled_address = ["tangled:4150"]
    writer = await ansq.create_writer(nsqd_tcp_addresses=tangled_address)

    data = [
        (uuid.uuid4().hex, [fake.binary(length=1024) for _message_count in range(50)])
        for _topic_count in range(10)
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
            assert message_actual.attempts == 1

        await reader.close()


@pytest.mark.asyncio
async def test_requeue():
    tangled_address = ["tangled:4150"]

    topic_name = uuid.uuid4().hex
    first_message = fake.binary(length=1024)
    second_message = fake.binary(length=1024)

    writer = await ansq.create_writer(nsqd_tcp_addresses=tangled_address)
    await writer.pub(topic=topic_name, message=first_message)
    await writer.pub(topic=topic_name, message=second_message)
    await writer.close()

    reader = await ansq.create_reader(
        nsqd_tcp_addresses=tangled_address,
        topic=topic_name,
        channel=uuid.uuid4().hex,
    )

    message_iterator = reader.messages()

    # getting the 'first message' for the first time
    message = await anext(message_iterator)
    assert message.body == first_message
    assert message.attempts == 1
    await message.req()

    # getting the 'second message' for the first time
    message = await anext(message_iterator)
    assert message.body == second_message
    assert message.attempts == 1
    await message.fin()

    # getting the 'first message' for the second time
    message = await anext(message_iterator)
    assert message.body == first_message
    assert message.attempts == 2
    await message.req()

    # getting the 'first message' for the third time
    message = await anext(message_iterator)
    assert message.body == first_message
    assert message.attempts == 3
    await message.fin()

    await reader.close()
