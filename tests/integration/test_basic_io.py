import asyncio
import dataclasses
import typing
import uuid

import ansq
import async_timeout
import faker
import pytest
from ansq.tcp.types import NSQMessage

fake = faker.Faker()


@dataclasses.dataclass
class ManyMessagesIterm:
    message_size: int
    message_count: int


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "test_item",
    [
        ManyMessagesIterm(message_size=16, message_count=10),
        ManyMessagesIterm(message_size=16, message_count=100),
        ManyMessagesIterm(message_size=16, message_count=300),
        ManyMessagesIterm(message_size=512, message_count=10),
        ManyMessagesIterm(message_size=512, message_count=100),
        ManyMessagesIterm(message_size=512, message_count=300),
        ManyMessagesIterm(message_size=1024, message_count=10),
        ManyMessagesIterm(message_size=1024, message_count=100),
        ManyMessagesIterm(message_size=1024, message_count=300),
        ManyMessagesIterm(message_size=1024 * 10, message_count=10),
        ManyMessagesIterm(message_size=1024 * 10, message_count=100),
        ManyMessagesIterm(message_size=1024 * 10, message_count=300),
    ],
)
async def test_write_many_messages_to_multiple_topics(test_item: ManyMessagesIterm):
    how_many_toics = 5
    how_many_channels = 5

    tangled_address = ["tangled:4150"]
    writer = await ansq.create_writer(nsqd_tcp_addresses=tangled_address)
    data = [
        (
            uuid.uuid4().hex,
            [
                fake.binary(length=test_item.message_size)
                for _message_count in range(test_item.message_count)
            ],
        )
        for _topic_count in range(how_many_toics)
    ]

    for topic_name, messages in data:
        readers = [
            await ansq.create_reader(
                nsqd_tcp_addresses=tangled_address,
                topic=topic_name,
                channel=uuid.uuid4().hex,
            )
            for reader_index in range(how_many_channels)
        ]

        for message in messages:
            await writer.pub(topic=topic_name, message=message)

        for reader in readers:
            for message_expected in messages:
                message_actual = await anext(reader.messages())
                await message_actual.fin()
                assert message_actual.body == message_expected
                assert message_actual.attempts == 1
            await reader.close()

    await writer.close()


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
    data = [fake.binary(length=128) for _ in range(10)]

    writer = await ansq.create_writer(nsqd_tcp_addresses=tangled_address)
    for message in data:
        await writer.pub(topic=topic_name, message=message)

    reader = await ansq.create_reader(
        nsqd_tcp_addresses=tangled_address,
        topic=topic_name,
        channel=uuid.uuid4().hex,
    )
    # ansq happen to set the RDY flag to 1, which means that only one message can be in flight at a time
    first_message = await anext(reader.messages())
    # The next message should not be available until at leas something is FINed or REQed
    assert await message_with_timeout(reader.messages()) is None
    # By finalizing the first message we unlock a new message to receive
    await first_message.fin()
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

    # getting the 'first message' for the second time
    # note, that we are getting the first message because it has higher priority
    message = await anext(message_iterator)
    assert message.body == first_message
    assert message.attempts == 2
    await message.fin()

    # getting the 'second message' for the first time
    message = await anext(message_iterator)
    assert message.body == second_message
    assert message.attempts == 1
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
