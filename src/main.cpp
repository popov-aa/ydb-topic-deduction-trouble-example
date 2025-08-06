#include <iostream>

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/table/table.h>
#include <ydb-cpp-sdk/client/topic/client.h>
#include <boost/program_options.hpp>
#include <boost/uuid/uuid.hpp>             // uuid class
#include <boost/uuid/uuid_generators.hpp>  // generators
#include <boost/uuid/uuid_io.hpp>          // streaming operators etc.

namespace po = boost::program_options;

namespace {
constexpr std::string_view kTopicPath{"ydb-topic-deduction-trouble-example-topic"};
constexpr std::string_view kTablePath{"ydb-topic-deduction-trouble-example-table"};
}  // namespace

// Функция создает сессию записи с переданной парой message_group_id/producer_id и отправляет сообщение в транзакции
void SendMessage(NYdb::NTopic::TTopicClient& topic_client, const std::string& message_group_id, const std::string& message,
                 NYdb::NTable::TTransaction* transaction) {
  std::cout << "Try to send row " << message << std::endl;
  auto write_session_settings = NYdb::NTopic::TWriteSessionSettings()
                                    .Path(std::string{kTopicPath})
                                    .ProducerId(message_group_id)
                                    .MessageGroupId(message_group_id)
                                    .DeduplicationEnabled(true);
  auto write_session = topic_client.CreateWriteSession(write_session_settings);

  bool sended{false};

  while (true) {
    for (const auto& event : write_session->GetEvents(false, {})) {
      if (auto* readyEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
        if (!sended) {
          write_session->Write(std::move(readyEvent->ContinuationToken), NYdb::NTopic::TWriteMessage(message), transaction);
          sended = true;
          std::cerr << std::format("Message was sent to topic: (topic_path={}, message={})", write_session_settings.Path_, message) << std::endl;
        }
      } else if (auto* ackEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TAcksEvent>(&event)) {
        for (const auto& ack : ackEvent->Acks) {
          if (ack.State == NYdb::NTopic::TWriteSessionEvent::TWriteAck::EEventState::EES_DISCARDED) {
            std::cerr << std::format("Ack discarded: (topic_path={}, message={})", write_session_settings.Path_, message) << std::endl;
            throw std::runtime_error(std::format("Failed to send message to topic {}: discarded", write_session_settings.Path_));
          } else {
            std::cerr << std::format("Ack {}: (topic_path={}, message={})", int(ack.State), write_session_settings.Path_, message) << std::endl;
          }
        }
        return;
      } else if (auto* closeSessionEvent = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event)) {
        std::cerr << std::format("Session closed: (topic_path={}, message={})", write_session_settings.Path_, message) << std::endl;
        throw std::runtime_error(closeSessionEvent->DebugString());
      } else {
        std::cerr << std::format("Unknown event: (topic_path={}, message={})", write_session_settings.Path_, message) << std::endl;
      }
    }
  }
  write_session->Close();
}

// Функция выполняет вставку в таблицу в транзакции
NYdb::TStatus InsertToTable(NYdb::NTable::TSession session, const std::string& id, const NYdb::NTable::TTransaction& transaction) {
  std::cout << "Try to insert row " << id << std::endl;
  static std::string query(std::format("INSERT INTO `{}` (id) VALUES ($id);", kTablePath));
  auto params = NYdb::TParamsBuilder().AddParam("$id").Utf8(id).Build().Build();
  auto tx_control = NYdb::NTable::TTxControl::Tx(transaction);
  auto query_result = session.ExecuteDataQuery(query, tx_control, params).GetValueSync();
  return query_result;
}

void WriteToTableAndSendMessage(NYdb::NTable::TTableClient table_client, NYdb::NTopic::TTopicClient& topic_client, std::vector<boost::uuids::uuid> uuids) {
  auto result = table_client.RetryOperationSync([&uuids, &topic_client](NYdb::NTable::TSession session) -> NYdb::TStatus {
    std::cout << "New transaction." << std::endl;

    auto transaction_result = session.BeginTransaction(NYdb::NTable::TTxSettings::SerializableRW()).GetValueSync();
    if (!transaction_result.IsSuccess()) {
      return transaction_result;
    }
    auto transaction = transaction_result.GetTransaction();

    for (const auto& uuid : uuids) {
      std::stringstream stream;
      stream << uuid;
      const auto uuid_string = stream.str();

      auto insert_status = InsertToTable(session, uuid_string, transaction);
      if (!insert_status.IsSuccess()) {
        return insert_status;
      }
      SendMessage(topic_client, uuid_string, uuid_string, &transaction);
    }
    auto commit_status = transaction.Commit().GetValueSync();
    if (!commit_status.IsSuccess()) {
      return commit_status;
    }
    return NYdb::TStatus{NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues{}};
  });

  if (!result.IsSuccess()) {
    std::cerr << "Failed to write and send message " << result.GetIssues().ToString() << std::endl;
  }
}

int main(int argc, char** argv) {
  try {
    po::options_description opt_desc("ydb-topic-deduction-trouble-example");

    std::string endpoint, database, user, password;
    opt_desc.add_options()                                                           //
        ("help", "Show help")                                                        //
        ("endpoint", po::value<std::string>(&endpoint)->default_value("127.0.0.1"))  //
        ("database", po::value<std::string>(&database)->default_value("/local"))     //
        ("user", po::value<std::string>(&user)->default_value("root"))               //
        ("password", po::value<std::string>(&password)->default_value("1234"));      //

    po::variables_map options_map;
    po::store(po::command_line_parser(argc, argv).options(opt_desc).extra_parser(po::ext_parser()).run(), options_map);

    if (options_map.count("help")) {
      std::cout << opt_desc << std::endl;
      return 0;
    }

    po::notify(options_map);

    // Подключение к БД
    auto driverConfig = NYdb::TDriverConfig().SetEndpoint(endpoint).SetDatabase(database).SetCredentialsProviderFactory(
        NYdb::CreateLoginCredentialsProviderFactory({user, password}));
    //.SetAuthToken(GetEnv("YDB_TOKEN")

    NYdb::TDriver driver(driverConfig);
    NYdb::NTable::TTableClient table_client(driver);
    NYdb::NQuery::TQueryClient query_client(driver);
    NYdb::NTopic::TTopicClient topic_client(driver);

    // Создаем таблицу
    auto create_table_status = query_client
                                   .ExecuteQuery(std::format(R"V0G0N(
CREATE TABLE IF NOT EXISTS `{}`
(
  id Utf8 NOT NULL,
  PRIMARY KEY (id)
) WITH (
  AUTO_PARTITIONING_BY_SIZE = ENABLED,
  AUTO_PARTITIONING_BY_LOAD = ENABLED,
  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 40,
  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100,
  AUTO_PARTITIONING_PARTITION_SIZE_MB = 1024
);
        )V0G0N",
                                                             kTablePath),
                                                 NYdb::NQuery::TTxControl::NoTx())
                                   .GetValueSync();
    if (!create_table_status.IsSuccess()) {
      throw std::runtime_error(std::format("error of creating table {}", create_table_status.GetIssues().ToString()));
    }

    // Создаем топик
    auto topic_settings = NYdb::NTopic::TCreateTopicSettings();
    auto create_topic_status = topic_client.CreateTopic(std::string{kTopicPath}, topic_settings).GetValueSync();

    // Назначаем консьюмера
    auto alter_topic_settings = NYdb::NTopic::TAlterTopicSettings().BeginAddConsumer("my-consumer").EndAddConsumer();
    auto alter_topic_status = topic_client.AlterTopic(std::string{kTopicPath}, alter_topic_settings).GetValueSync();

    // Записываем сообщения

    static boost::uuids::random_generator random_generator;
    WriteToTableAndSendMessage(table_client, topic_client, {random_generator()});
    WriteToTableAndSendMessage(table_client, topic_client, {random_generator(), random_generator()});
    WriteToTableAndSendMessage(table_client, topic_client, {random_generator(), random_generator(), random_generator()});
  } catch (const std::exception& ex) {
    std::cerr << std::format("Catch exception {}", ex.what()) << std::endl;
  }
  return 0;
}
