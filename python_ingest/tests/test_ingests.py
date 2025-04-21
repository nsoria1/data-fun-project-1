import json
from unittest.mock import MagicMock, patch, ANY
from python_ingest.src.main import ingest_to_kafka

SAMPLE = """\
{"name":"Not Again SU","identifier":76716259,"version":{"identifier":1220413474,"comment":"Added # to title.","is_minor_edit":true,"scores":{"revertrisk":{"probability":{"false":0.5951822996139526,"true":0.40481770038604736}}},"editor":{"identifier":47559838,"name":"T.olivia.p","edit_count":4,"groups":["*","user"],"date_started":"2024-03-19T15:34:53Z"},"number_of_characters":72,"size":{"value":72,"unit_text":"B"},"maintenance_tags":{}},"event":{"identifier":"fa3cae5b-8707-4f55-9fab-5baa4c286a32","type":"update","date_created":"2024-04-23T16:53:56.207887Z","date_published":"2024-08-06T08:45:21.6690873Z"},"url":"https://en.wikipedia.org/wiki/Not_Again_SU","date_created":"2024-04-23T16:50:32Z","date_modified":"2024-04-23T16:53:55Z","is_part_of":{"identifier":"enwiki","url":"https://en.wikipedia.org"},"in_language":{"identifier":"en"},"license":[{"name":"Creative Commons Attribution-ShareAlike 4.0","identifier":"CC-BY-SA-4.0","url":"https://creativecommons.org/licenses/by-sa/4.0/"}],"description":"Student organization in Syracuse, New York","sections":[{"type":"section","name":"Abstract"},{"type":"section","name":"Introduction:","has_parts":[{"type":"paragraph","value":"The hashtag and student led organization #NotAgainSU began circulating after several racist incidents occurred on the campus of Syracuse University, during the academic course of 2019-2021. Initial reports of racist paraphernalia occurred early November in the dorm buildings of first- and second-year undergraduate students. After nearly a month of daily reports, Syracuse students organized a sit-in where they occupied, and created an elaborate list of 19 demands for Chancellor Kent Syverud to sign. After several protests, some taking place at his persona residence, the list was signed with few revisions on November 21st."}]}]}
{"name":"Mike Ross (Suits)","identifier":76727681,"abstract":"REDIRECT List of Suits characters#Mike Ross","version":{"identifier":1220574416,"comment":"Requesting speedy deletion ([[WP:CSD#R2|CSD R2]]).","tags":["mw-removed-redirect","twinkle"],"scores":{"revertrisk":{"probability":{"false":0.8686162978410721,"true":0.13138370215892792}}},"editor":{"identifier":9560390,"name":"Launchballer","edit_count":29828,"groups":["autoreviewer","extendedconfirmed","extendedmover","reviewer","rollbacker","*","user","autoconfirmed"],"is_patroller":true,"date_started":"2009-04-28T14:46:05Z"},"number_of_characters":141,"size":{"value":141,"unit_text":"B"},"noindex":true,"maintenance_tags":{}},"event":{"identifier":"22366970-ddf7-43dc-8cec-941de1248b57","type":"update","date_created":"2024-04-24T17:14:11.475093Z","date_published":"2024-08-06T08:45:21.875527Z"},"url":"https://en.wikipedia.org/wiki/Mike_Ross_(Suits)","date_created":"2024-04-24T17:12:05Z","date_modified":"2024-04-24T17:14:10Z","main_entity":{"identifier":"Q125388612","url":"https://www.wikidata.org/entity/Q125388612"},"is_part_of":{"identifier":"enwiki","url":"https://en.wikipedia.org"},"additional_entities":[{"identifier":"Q125388612","url":"https://www.wikidata.org/entity/Q125388612","aspects":["S"]}],"in_language":{"identifier":"en"},"license":[{"name":"Creative Commons Attribution-ShareAlike 4.0","identifier":"CC-BY-SA-4.0","url":"https://creativecommons.org/licenses/by-sa/4.0/"}],"description":"An American Legal drama series from 2011-2019","sections":[{"type":"section","name":"Abstract","has_parts":[{"type":"list","has_parts":[{"type":"list_item","value":"REDIRECT List of Suits characters#Mike Ross","links":[{"url":"https://en.wikipedia.org/wiki/List_of_Suits_characters#Mike_Ross","text":"List of Suits characters#Mike Ross"}]}]}]}]}
"""


@patch("python_ingest.src.main.Producer")
def test_ingest_real_jsonl(mock_producer_cls, tmp_path, monkeypatch):
    # Write sample file to temp path
    file_path = tmp_path / "real.jsonl"
    file_path.write_text(SAMPLE)

    # Configure env variables
    monkeypatch.setenv("FILE_TO_READ", str(file_path))
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "fake:9092")
    monkeypatch.setenv("INGEST_TOPIC", "wikiedits")  # Cambiado de KAFKA_TOPIC a INGEST_TOPIC
    monkeypatch.setenv("MAX_MESSAGES", "2")  # Añadido MAX_MESSAGES

    # Mock the producer
    mock_producer = MagicMock()
    mock_producer_cls.return_value = mock_producer

    # Simular valor de retorno para flush (0 mensajes pendientes)
    mock_producer.flush.return_value = 0

    # Execution of the method
    ingest_to_kafka()

    # Verificar la configuración del productor
    mock_producer_cls.assert_called_once_with({
        "bootstrap.servers": "fake:9092",
        "client.id": "test_nico",
        "compression.type": "gzip"
    })

    # Make sure there is two calls to produce
    assert mock_producer.produce.call_count == 2

    # Verify callback is included into the call
    for call in mock_producer.produce.call_args_list:
        assert "callback" in call.kwargs
        assert call.kwargs["callback"].__name__ == "delivery_report"

    # Test first record with key name
    first_val = mock_producer.produce.call_args_list[0].kwargs["value"]
    first_obj = json.loads(first_val.decode("utf-8"))
    assert first_obj["name"] == "Not Again SU"

    # Test second record with abstract assert
    second_obj = json.loads(
        mock_producer.produce.call_args_list[1].kwargs["value"].decode("utf-8")
    )
    assert "abstract" in second_obj

    # Check poll call count
    assert mock_producer.poll.call_count >= 1

    # Verify flush has been called with the right timeout
    mock_producer.flush.assert_called_once_with(timeout=30)


@patch("python_ingest.src.main.Producer")
def test_ingest_with_max_messages(mock_producer_cls, tmp_path, monkeypatch):
    file_path = tmp_path / "limited.jsonl"
    file_path.write_text(SAMPLE)

    # Configure to process 1 message
    monkeypatch.setenv("FILE_TO_READ", str(file_path))
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "fake:9092")
    monkeypatch.setenv("INGEST_TOPIC", "wikiedits")
    monkeypatch.setenv("MAX_MESSAGES", "1")  # Solo procesar 1 mensaje

    # Producer mock
    mock_producer = MagicMock()
    mock_producer_cls.return_value = mock_producer
    mock_producer.flush.return_value = 0

    # Execute
    ingest_to_kafka()

    # Check it has been processed once
    assert mock_producer.produce.call_count == 1


@patch("python_ingest.src.main.Producer")
def test_handle_json_decode_error(mock_producer_cls, tmp_path, monkeypatch):
    invalid_json = '{"name": "Valid JSON"}\n{"name": Invalid JSON}\n{"name": "Valid Again"}'
    file_path = tmp_path / "invalid.jsonl"
    file_path.write_text(invalid_json)

    # Configure variables
    monkeypatch.setenv("FILE_TO_READ", str(file_path))
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "fake:9092")
    monkeypatch.setenv("INGEST_TOPIC", "wikiedits")
    monkeypatch.setenv("MAX_MESSAGES", "3")

    # Producer mock
    mock_producer = MagicMock()
    mock_producer_cls.return_value = mock_producer
    mock_producer.flush.return_value = 0

    # Execute
    ingest_to_kafka()

    # Check only 2 calls were done
    assert mock_producer.produce.call_count == 2