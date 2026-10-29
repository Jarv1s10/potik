"""How Config builds client options."""

from potik import Config


def test_static_transactional_ids_keep_the_prefix_and_differ_per_group() -> None:
    def transactional_id(group_id: str, instance: str) -> str:
        config = Config(
            bootstrap_servers="broker:9092",
            group_id=group_id,
            transactional_id_prefix="orders",
            consumer_options={"group.instance.id": instance},
        )
        return config._producer_config(lambda _error, _message: None)["transactional.id"]

    assert transactional_id("a-b", "c") == "orders-3-a-b-c"
    assert transactional_id("a", "b-c") == "orders-1-a-b-c"
