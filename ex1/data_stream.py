from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._counter: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("No data available in processor")
        extracted_data: str = self._storage.pop(0)
        rank: int = self._counter
        return rank, extracted_data


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list) and data:
            return all(isinstance(x, (int, float)) for x in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._storage.append(str(item))
                self._counter += 1
        else:
            self._storage.append(str(data))
            self._counter += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list) and data:
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            for item in data:
                self._storage.append(item)
                self._counter += 1
        else:
            self._storage.append(data)
            self._counter += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(isinstance(k, str) and
                       isinstance(v, str) for k, v in data.items())
        if isinstance(data, list) and data:
            return all(
                isinstance(item, dict) and
                all(isinstance(k, str) and
                    isinstance(v, str) for k, v in item.items())
                for item in data
            )
        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        def format_log(entry: dict[str, str]) -> str:
            level = entry.get("log_level", "UNKNOWN")
            msg = entry.get("log_message", "")
            return f"{level}: {msg}"

        if isinstance(data, list):
            for item in data:
                self._storage.append(format_log(item))
                self._counter += 1
        else:
            self._storage.append(format_log(data))
            self._counter += 1


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            routed = False
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    routed = True
                    break
            if not routed:
                print(f"DataStream error - Can't process element in stream: "
                      f"{element}")

    def print_processors_stats(self) -> None:
        print("=== DataStream Statistics ===")
        if not self._processors:
            print("No processor found, no data")
            return

        for proc in self._processors:
            name = proc.__class__.__name__
            if name == "NumericProcessor" or name == "TextProcessor":
                name = name.replace("Processor", " Processor")
            elif name == "LogProcessor":
                name = name.replace("Processor", " Processor")

            total = proc._counter
            remaining = len(proc._storage)
            print(f"{name}: total {total} items processed, "
                  f"remaining {remaining} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print("\nInitialize Data Stream...")

    data_stream = DataStream()

    data_stream.print_processors_stats()

    num_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()

    print("\nRegistering Numeric Processor\n")
    data_stream.register_processor(num_processor)

    batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING',
             'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}
        ],
        42,
        ['Hi', 'five']
    ]

    print("Send first batch of data on stream: "
          "['Hello world', [3.14, -1, 2.71], [{'log_level': 'WARNING', '\n"
          " log_message': 'Telnet access! Use ssh instead'}, "
          "{'log_level': 'INFO', 'log_message': 'User wil is\n"
          "connected'}], 42, ['Hi', 'five']]")

    data_stream.process_stream(batch)
    data_stream.print_processors_stats()

    print("\nRegistering other data processors")
    data_stream.register_processor(text_processor)
    data_stream.register_processor(log_processor)

    print("Send the same batch again")
    data_stream.process_stream(batch)
    data_stream.print_processors_stats()

    print("\nConsume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    for _ in range(3):
        num_processor.output()
    for _ in range(2):
        text_processor.output()
    for _ in range(1):
        log_processor.output()

    data_stream.print_processors_stats()
