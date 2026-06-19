from abc import ABC, abstractmethod
from typing import Any, Protocol


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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        raw_values = [val for _, val in data]
        csv_string = ",".join(raw_values)
        print(csv_string)


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        json_parts = []
        for rank, val in data:
            clean_val = val.replace('\n', '\\n')
            json_parts.append(f'"item_{rank}": "{clean_val}"')
        json_string = "{" + ". ".join(json_parts) + "}"
        print(json_string)


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
        print("\n=== DataStream Statistics ===")
        if not self._processors:
            print("No processor found, no data")
            return

        for proc in self._processors:
            name = proc.__class__.__name__
            if name in ("NumericProcessor", "TextProcessor", "LogProcessor"):
                name = name.replace("Processor", " Processor")

            total = proc._counter
            remaining = len(proc._storage)
            print(f"{name}: total {total} items processed, "
                  f"remaining {remaining} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            extracted_list: list[tuple[int, str]] = []
            for _ in range(nb):
                if not proc._storage:
                    break
                try:
                    rank, val = proc.output()
                    extracted_list.append((rank, val))
                except IndexError:
                    break

            if extracted_list:
                plugin.process_output(extracted_list)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...\n")

    data_stream = DataStream()
    data_stream.print_processors_stats()

    print("\nRegistering Processors\n")
    num_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()

    data_stream.register_processor(num_processor)
    data_stream.register_processor(text_processor)
    data_stream.register_processor(log_processor)

    batch_1 = [
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
          "['Hello world', [3.14, -1, 2.71], "
          "[{'log_level': 'WARNING', "
          "'\nlog_message': 'Telnet access! Use ssh instead'}, "
          "{'log_level': 'INFO', "
          "'log_message': 'User wil is\nconnected'}], 42, ['Hi', 'five']]")

    data_stream.process_stream(batch_1)
    data_stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    data_stream.output_pipeline(3, csv_plugin)
    data_stream.print_processors_stats()

    batch_2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [
            {'log_level': 'ERROR', 'log_message': '500 server crash'},
            {'log_level': 'NOTICE',
             'log_message': 'Certificate expires in 10 days'}
        ],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]

    print("\nSend another batch of data: "
          "[21, ['I love AI', 'LLMs are wonderful', "
          "'Stay healthy'], [{'log_level': '\nERROR', "
          "'log_message': '500 server crash'}, "
          "{'log_level': 'NOTICE', "
          "'log_message': 'Certificate\nexpires in 10 days'}], "
          "[32, 42, 64, 84, 128, 168], 'World hello']")

    data_stream.process_stream(batch_2)
    data_stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExportPlugin()
    data_stream.output_pipeline(5, json_plugin)
    data_stream.print_processors_stats()
