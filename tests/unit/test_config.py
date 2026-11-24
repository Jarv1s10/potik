"""How Config builds client options."""

from potik import Config, Producer


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


def test_producers_partition_keys_as_the_java_client_unless_told_otherwise() -> None:
    def partitioner(config: Config) -> str:
        return config._producer_config(lambda _error, _message: None)["partitioner"]

    assert partitioner(Config(bootstrap_servers="broker:9092", group_id="g")) == "murmur2_random"
    assert Producer("broker:9092")._config["partitioner"] == "murmur2_random"
    for config in (
        Config(bootstrap_servers="broker:9092", group_id="g", producer_options={"partitioner": "consistent_random"}),
        Config(bootstrap_servers="broker:9092", group_id="g", options={"partitioner": "consistent_random"}),
    ):
        assert partitioner(config) == "consistent_random"
    assert Producer("broker:9092", options={"partitioner": "random"})._config["partitioner"] == "random"
    config = Config(bootstrap_servers="broker:9092", group_id="g")
    assert "partitioner" not in config.client_config()
    assert "partitioner" not in config._consumer_config()
