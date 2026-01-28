def test_register_and_token(client, user):
    # Фикстура user уже делает register + token
    assert user.email.endswith('@test.com')
    assert user.token and len(user.token) > 10
