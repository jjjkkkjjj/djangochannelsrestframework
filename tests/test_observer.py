import asyncio

import pytest
from asgiref.sync import async_to_sync
from channels import DEFAULT_CHANNEL_LAYER
from channels.db import database_sync_to_async
from channels.layers import channel_layers
from channels.testing import WebsocketCommunicator
from django.contrib.auth import get_user_model, user_logged_in
from django.db import transaction
from django.utils.text import slugify
from rest_framework import serializers

from djangochannelsrestframework.consumers import AsyncAPIConsumer
from djangochannelsrestframework.decorators import action
from djangochannelsrestframework.observer import model_observer, observer


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_observer_wrapper(settings):
    settings.CHANNEL_LAYERS = {
        'default': {
            'BACKEND': 'channels.layers.InMemoryChannelLayer',
            'TEST_CONFIG': {
                'expiry': 100500,
            },
        },
    }

    layer = channel_layers.make_test_backend(DEFAULT_CHANNEL_LAYER)

    class TestConsumer(AsyncAPIConsumer):
        async def accept(self, **kwargs):
            await self.handle_user_logged_in.subscribe()
            await super().accept()

        @observer(user_logged_in)
        async def handle_user_logged_in(self, message, observer=None, **kwargs):
            await self.send_json({'message': message, 'observer': observer is not None})

    communicator = WebsocketCommunicator(TestConsumer(), '/testws/')

    connected, _ = await communicator.connect()

    assert connected

    user = await database_sync_to_async(get_user_model().objects.create)(
        username='test', email='test@example.com'
    )

    await database_sync_to_async(user_logged_in.send)(
        sender=user.__class__, request=None, user=user
    )

    response = await communicator.receive_json_from()

    assert response == {'message': {}, 'observer': True}

    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_model_observer_wrapper(settings):
    settings.CHANNEL_LAYERS = {
        'default': {
            'BACKEND': 'channels.layers.InMemoryChannelLayer',
            'TEST_CONFIG': {
                'expiry': 100500,
            },
        },
    }

    layer = channel_layers.make_test_backend(DEFAULT_CHANNEL_LAYER)

    class TestConsumer(AsyncAPIConsumer):
        async def accept(self, **kwargs):
            await self.user_change_observer_wrapper.subscribe()
            await super().accept()

        @model_observer(get_user_model())
        async def user_change_observer_wrapper(
            self, message, action, message_type, observer=None, **kwargs
        ):
            await self.send_json(dict(body=message, action=action, type=message_type))

    communicator = WebsocketCommunicator(TestConsumer(), '/testws/')

    connected, _ = await communicator.connect()

    assert connected

    user = await database_sync_to_async(get_user_model().objects.create)(
        username='test', email='test@example.com'
    )

    response = await communicator.receive_json_from()

    assert response == {
        'action': 'create',
        'body': {'pk': user.pk},
        'type': 'user.change.observer.wrapper',
    }

    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_model_observer_wrapper_in_transaction(settings):
    settings.CHANNEL_LAYERS = {
        'default': {
            'BACKEND': 'channels.layers.InMemoryChannelLayer',
            'TEST_CONFIG': {
                'expiry': 100500,
            },
        },
    }

    layer = channel_layers.make_test_backend(DEFAULT_CHANNEL_LAYER)

    class TestConsumer(AsyncAPIConsumer):
        async def accept(self, **kwargs):
            await TestConsumer.user_change_wrapper_in_transaction.subscribe(self)
            await super().accept()

        @model_observer(get_user_model())
        async def user_change_wrapper_in_transaction(
            self, message, action, message_type, observer=None, **kwargs
        ):
            await self.send_json(dict(body=message, action=action, type=message_type))

    communicator = WebsocketCommunicator(TestConsumer(), '/testws/')

    connected, _ = await communicator.connect()

    assert connected

    @database_sync_to_async
    def create_user_and_wait():

        with transaction.atomic():
            user = get_user_model().objects.create(
                username='test', email='test@example.com'
            )
            assert async_to_sync(communicator.receive_nothing(timeout=0.1))
            user.username = 'mike'
            user.save()
            assert async_to_sync(communicator.receive_nothing(timeout=0.1))
            return user

    user = await create_user_and_wait()

    response = await communicator.receive_json_from()

    assert response == {
        'action': 'create',
        'body': {'pk': user.pk},
        'type': 'user.change.wrapper.in.transaction',
    }

    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_model_observer_delete_wrapper(settings):
    settings.CHANNEL_LAYERS = {
        'default': {
            'BACKEND': 'channels.layers.InMemoryChannelLayer',
            'TEST_CONFIG': {
                'expiry': 100500,
            },
        },
    }

    layer = channel_layers.make_test_backend(DEFAULT_CHANNEL_LAYER)

    class TestConsumerObserverDelete(AsyncAPIConsumer):
        async def accept(self, **kwargs):
            await self.user_change_observer_delete.subscribe()
            await super().accept()

        @model_observer(get_user_model())
        async def user_change_observer_delete(
            self, message, action, message_type, observer=None, **kwargs
        ):
            await self.send_json(dict(body=message, action=action, type=message_type))

    communicator = WebsocketCommunicator(TestConsumerObserverDelete(), '/testws/')

    connected, _ = await communicator.connect()

    assert connected
    await communicator.receive_nothing()

    user = await database_sync_to_async(get_user_model())(
        username='test', email='test@example.com'
    )
    await database_sync_to_async(user.save)()

    response = await communicator.receive_json_from()
    await communicator.receive_nothing()

    assert response == {
        'action': 'create',
        'body': {'pk': user.pk},
        'type': 'user.change.observer.delete',
    }
    pk = user.pk

    await database_sync_to_async(user.delete)()

    response = await communicator.receive_json_from()

    await communicator.receive_nothing()

    assert response == {
        'action': 'delete',
        'body': {'pk': pk},
        'type': 'user.change.observer.delete',
    }

    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_model_observer_many_connections_wrapper(settings):
    settings.CHANNEL_LAYERS = {
        'default': {
            'BACKEND': 'channels.layers.InMemoryChannelLayer',
            'TEST_CONFIG': {
                'expiry': 100500,
            },
        },
    }

    layer = channel_layers.make_test_backend(DEFAULT_CHANNEL_LAYER)

    class TestConsumer(AsyncAPIConsumer):
        async def accept(self, **kwargs):
            await self.user_change_many_connections_wrapper.subscribe()
            await super().accept()

        @model_observer(get_user_model())
        async def user_change_many_connections_wrapper(
            self, message, action, message_type, observer=None, **kwargs
        ):
            await self.send_json(dict(body=message, action=action, type=message_type))

    communicator1 = WebsocketCommunicator(TestConsumer(), '/testws/')

    connected, _ = await communicator1.connect()

    assert connected

    communicator2 = WebsocketCommunicator(TestConsumer(), '/testws/')

    connected, _ = await communicator2.connect()

    assert connected

    user = await database_sync_to_async(get_user_model().objects.create)(
        username='test', email='test@example.com'
    )

    response = await communicator1.receive_json_from()

    assert response == {
        'action': 'create',
        'body': {'pk': user.pk},
        'type': 'user.change.many.connections.wrapper',
    }

    await communicator1.disconnect()

    response = await communicator2.receive_json_from()

    assert response == {
        'action': 'create',
        'body': {'pk': user.pk},
        'type': 'user.change.many.connections.wrapper',
    }

    await communicator2.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_model_observer_many_consumers_wrapper(settings):
    settings.CHANNEL_LAYERS = {
        'default': {
            'BACKEND': 'channels.layers.InMemoryChannelLayer',
            'TEST_CONFIG': {
                'expiry': 100500,
            },
        },
    }

    layer = channel_layers.make_test_backend(DEFAULT_CHANNEL_LAYER)

    class TestConsumer(AsyncAPIConsumer):
        async def accept(self, **kwargs):
            await self.user_change_many_consumers_wrapper_1.subscribe()
            await super().accept()

        @model_observer(get_user_model())
        async def user_change_many_consumers_wrapper_1(
            self, message, action, message_type, observer=None, **kwargs
        ):
            await self.send_json(dict(body=message, action=action, type=message_type))

    class TestConsumer2(AsyncAPIConsumer):
        async def accept(self, **kwargs):
            await self.user_change_many_consumers_wrapper_2.subscribe()
            await super().accept()

        @model_observer(get_user_model())
        async def user_change_many_consumers_wrapper_2(
            self, message, action, message_type, observer=None, **kwargs
        ):
            await self.send_json(dict(body=message, action=action, type=message_type))

    communicator1 = WebsocketCommunicator(TestConsumer(), '/testws/')

    connected, _ = await communicator1.connect()

    assert connected

    communicator2 = WebsocketCommunicator(TestConsumer2(), '/testws/')

    connected, _ = await communicator2.connect()

    assert connected

    user = await database_sync_to_async(get_user_model().objects.create)(
        username='test', email='test@example.com'
    )

    response = await communicator1.receive_json_from()

    assert response == {
        'action': 'create',
        'body': {'pk': user.pk},
        'type': 'user.change.many.consumers.wrapper.1',
    }

    await communicator1.disconnect()

    response = await communicator2.receive_json_from()

    assert response == {
        'action': 'create',
        'body': {'pk': user.pk},
        'type': 'user.change.many.consumers.wrapper.2',
    }

    await communicator2.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_model_observer_custom_groups_wrapper(settings):
    settings.CHANNEL_LAYERS = {
        'default': {
            'BACKEND': 'channels.layers.InMemoryChannelLayer',
            'TEST_CONFIG': {
                'expiry': 100500,
            },
        },
    }

    layer = channel_layers.make_test_backend(DEFAULT_CHANNEL_LAYER)

    class TestConsumer(AsyncAPIConsumer):
        async def accept(self, **kwargs):
            await self.user_change_custom_groups_wrapper.subscribe(username='test')
            await super().accept()

        @model_observer(get_user_model())
        async def user_change_custom_groups_wrapper(
            self, message, action, message_type, observer=None, **kwargs
        ):
            await self.send_json(dict(body=message, action=action, type=message_type))

        @user_change_custom_groups_wrapper.groups
        def user_change_custom_groups_wrapper(
            self, instance=None, username=None, **kwargs
        ):
            if username:
                yield f'-instance-username-{slugify(username)}'
            else:
                yield f'-instance-username-{instance.username}'

    communicator = WebsocketCommunicator(TestConsumer(), '/testws/')

    connected, _ = await communicator.connect()

    assert connected

    user = await database_sync_to_async(get_user_model().objects.create)(
        username='test', email='test@example.com'
    )

    response = await communicator.receive_json_from()

    assert response == {
        'action': 'create',
        'body': {'pk': user.pk},
        'type': 'user.change.custom.groups.wrapper',
    }

    await communicator.disconnect()

    user = await database_sync_to_async(get_user_model().objects.create)(
        username='test2', email='test@example.com'
    )

    # no event since this is only subscribed to 'test'
    with pytest.raises(asyncio.TimeoutError):
        await communicator.receive_json_from()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_model_observer_with_class_serializer(settings):
    settings.CHANNEL_LAYERS = {
        'default': {
            'BACKEND': 'channels.layers.InMemoryChannelLayer',
            'TEST_CONFIG': {
                'expiry': 100500,
            },
        },
    }

    layer = channel_layers.make_test_backend(DEFAULT_CHANNEL_LAYER)

    class UserSerializer(serializers.ModelSerializer):
        class Meta:
            model = get_user_model()
            fields = ['id', 'username']

    class TestConsumerObserverUsers(AsyncAPIConsumer):
        async def accept(self, **kwargs):
            await self.users_changes.subscribe()
            await super().accept()

        @model_observer(get_user_model(), serializer_class=UserSerializer)
        async def users_changes(self, message, action, **kwargs):
            await self.reply(data=message, action=action)

    communicator = WebsocketCommunicator(TestConsumerObserverUsers(), '/testws/')

    connected, _ = await communicator.connect()

    assert connected

    user = await database_sync_to_async(get_user_model().objects.create)(
        username='test', email='test@example.com'
    )

    response = await communicator.receive_json_from()

    assert response == {
        'action': 'create',
        'response_status': 200,
        'request_id': None,
        'errors': [],
        'data': {
            'id': user.pk,
            'username': user.username,
        },
    }

    user.username = 'test updated'
    await database_sync_to_async(user.save)()

    response = await communicator.receive_json_from()

    assert response == {
        'action': 'update',
        'response_status': 200,
        'request_id': None,
        'errors': [],
        'data': {
            'id': user.pk,
            'username': user.username,
        },
    }

    pk = user.pk
    await database_sync_to_async(user.delete)()

    response = await communicator.receive_json_from()

    assert response == {
        'action': 'delete',
        'response_status': 200,
        'request_id': None,
        'errors': [],
        'data': {
            'id': pk,
            'username': user.username,
        },
    }

    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_model_observer_custom_groups_wrapper_with_split_function_api(settings):
    settings.CHANNEL_LAYERS = {
        'default': {
            'BACKEND': 'channels.layers.InMemoryChannelLayer',
            'TEST_CONFIG': {
                'expiry': 100500,
            },
        },
    }

    layer = channel_layers.make_test_backend(DEFAULT_CHANNEL_LAYER)

    class TestConsumerObserverCustomGroups(AsyncAPIConsumer):
        async def accept(self, **kwargs):
            await self.user_change_custom_groups.subscribe(username='test')
            await super().accept()

        @model_observer(get_user_model())
        async def user_change_custom_groups(
            self, message, action, message_type, observer=None, **kwargs
        ):
            await self.send_json(dict(body=message, action=action, type=message_type))

        @user_change_custom_groups.groups_for_signal
        def user_change_custom_groups(self, instance=None, **kwargs):
            yield f'-instance-username-{instance.username}'

        @user_change_custom_groups.groups_for_consumer
        def user_change_custom_groups(self, username=None, **kwargs):
            yield f'-instance-username-{slugify(username)}'

    communicator = WebsocketCommunicator(TestConsumerObserverCustomGroups(), '/testws/')

    connected, _ = await communicator.connect()

    assert connected

    user = await database_sync_to_async(get_user_model().objects.create)(
        username='test', email='test@example.com'
    )

    response = await communicator.receive_json_from()

    assert response == {
        'action': 'create',
        'body': {'pk': user.pk},
        'type': 'user.change.custom.groups',
    }

    await communicator.disconnect()

    user = await database_sync_to_async(get_user_model().objects.create)(
        username='test2', email='test@example.com'
    )

    # no event since this is only subscribed to 'test'
    with pytest.raises(asyncio.TimeoutError):
        await communicator.receive_json_from()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_model_observer_with_request_id(settings):
    settings.CHANNEL_LAYERS = {
        'default': {
            'BACKEND': 'channels.layers.InMemoryChannelLayer',
            'TEST_CONFIG': {
                'expiry': 100500,
            },
        },
    }

    layer = channel_layers.make_test_backend(DEFAULT_CHANNEL_LAYER)

    class TestConsumerObserverCustomGroups(AsyncAPIConsumer):
        @action()
        async def subscribe(self, username, request_id, **kwargs):
            await self.user_change_custom_groups.subscribe(
                username=username, request_id=request_id
            )

        @model_observer(get_user_model())
        async def user_change_custom_groups(
            self,
            message,
            action,
            message_type,
            observer=None,
            subscribing_request_ids=None,
            **kwargs,
        ):
            await self.send_json(
                dict(
                    body=message,
                    action=action,
                    type=message_type,
                    subscribing_request_ids=subscribing_request_ids,
                )
            )

        @user_change_custom_groups.groups_for_signal
        def user_change_custom_groups(self, instance=None, **kwargs):
            yield f'-instance-username-{instance.username}'

        @user_change_custom_groups.groups_for_consumer
        def user_change_custom_groups(self, username=None, **kwargs):
            yield f'-instance-username-{slugify(username)}'

    communicator = WebsocketCommunicator(TestConsumerObserverCustomGroups(), '/testws/')

    connected, _ = await communicator.connect()

    assert connected

    await communicator.send_json_to(
        {
            'action': 'subscribe',
            'username': 'thenewname',
            'request_id': 5,
        }
    )

    user = await database_sync_to_async(get_user_model().objects.create)(
        username='thenewname', email='test@example.com'
    )

    response = await communicator.receive_json_from()

    assert response == {
        'action': 'create',
        'body': {'pk': user.pk},
        'type': 'user.change.custom.groups',
        'subscribing_request_ids': [5],
    }

    await communicator.disconnect()
