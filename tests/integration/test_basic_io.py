import asyncio
import typing
import uuid

import ansq
import async_timeout
import faker
import pytest
from ansq.tcp.types import NSQMessage

fake = faker.Faker()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message_len,message_count",
    [
        # (0, 100),
        (1024, 50),
        (1024, 1000),
    ],
)
async def test_write_many_messages_to_multiple_topics(message_len, message_count):
    tangled_address = ["tangled:4150"]
    writer = await ansq.create_writer(nsqd_tcp_addresses=tangled_address)

    data = [
        (
            uuid.uuid4().hex,
            [
                fake.binary(length=message_len)
                for _message_count in range(message_count)
            ],
        )
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


async def message_with_timeout(
    messages: typing.AsyncIterator[NSQMessage], timeout: float = 0.1
) -> typing.Optional[NSQMessage]:
    try:
        async with async_timeout.timeout(timeout):
            return await anext(messages)
    except asyncio.TimeoutError:
        return None


@pytest.mark.asyncio
async def test_max_in_flight():
    tangled_address = ["tangled:4150"]

    topic_name = uuid.uuid4().hex
    data = [fake.binary(length=1024) for _ in range(40)]

    writer = await ansq.create_writer(nsqd_tcp_addresses=tangled_address)
    for message in data:
        await writer.pub(topic=topic_name, message=message)

    reader = await ansq.create_reader(
        nsqd_tcp_addresses=tangled_address,
        topic=topic_name,
        channel=uuid.uuid4().hex,
    )

    messages = reader.messages()

    # The first <max-in-flight: 16> messages should be available without acknoledgment
    messages_first_batch = [await anext(messages) for _ in range(16)]

    # The next message should not be available until at leas something is FINed or REQed
    assert await message_with_timeout(reader.messages()) is None

    # By finalizing the first message we unlock a new message to receive
    await messages_first_batch[0].fin()

    # Now this call should yield a new message insteand of None
    assert await message_with_timeout(reader.messages()) is not None

    # And the next attempt should fail again
    assert await message_with_timeout(reader.messages()) is None

    await reader.close()
    await writer.close()


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message_len,messages_count",
    [
        # tiny messages
        # MPUBing just a single message seems to be broken in ansq
        # (8, 1),
        (8, 8),
        (8, 100),
        (8, 10000),
        (8, 100000),
        # med messages
        # (128, 1),
        (128, 8),
        (128, 100),
        (128, 10000),
        (2048, 8),
        (2048, 100),
        (2048, 1000),
        # huge messages
        # (2048000, 1),
        (2048000, 4),
        (2048000, 8),
    ],
)
async def test_mpub(message_len, messages_count):
    tangled_address = ["tangled:4150"]

    topic_name = uuid.uuid4().hex
    messages_expected = [fake.binary(length=message_len) for _ in range(messages_count)]
    writer = await ansq.create_writer(nsqd_tcp_addresses=tangled_address)

    await writer.mpub(topic_name, *messages_expected)
    await writer.close()

    reader = await ansq.create_reader(
        nsqd_tcp_addresses=tangled_address,
        topic=topic_name,
        channel=uuid.uuid4().hex,
    )

    actual_messages = reader.messages()
    for expected_message in messages_expected:
        actual_message = await message_with_timeout(actual_messages)
        assert actual_message is not None
        assert actual_message.body == expected_message
        await actual_message.fin()

    await reader.close()
