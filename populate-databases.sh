#!/bin/bash

# Configuration
rooms=("alpha" "bravo" "golf" "hotel" "mike" "november" "sierra" "tango")
user="Leon"
message_count=100
msg_size_kb=32

# Base URLs
create_room_url="http://localhost:80/v1/addroom"
send_message_url="http://localhost:80/v1/sendmessage"

# Generate large message content once
msg_content=$(head -c $(($msg_size_kb * 1024)) < /dev/zero | tr '\0' 'X')

# Metrics variables
total_messages=0
total_data_mb=0
write_start_time=""
write_end_time=""

echo "==============================================="
echo "LOAD TEST CONFIGURATION"
echo "==============================================="
echo "Rooms: ${#rooms[@]}"
echo "Messages per room: $message_count"
echo "Message size: ${msg_size_kb} KB"
echo "Total messages to send: $((${#rooms[@]} * message_count))"
echo "Total data size: $(echo "scale=2; ${#rooms[@]} * $message_count * $msg_size_kb / 1024" | bc) MB"
echo "==============================================="
echo

echo "== Creating Rooms =="
for room in "${rooms[@]}"; do

  curl --request POST \
    --url "$create_room_url" \
    --header 'Content-Type: application/json' \
    --data '{
      "name": "'"$room"'",
      "allowed_users": ["'"$user"'"]
    }' \
    --silent --write-out "Room $room - HTTP %{http_code}\n" --output /dev/null

  sleep 0.3
done

echo "== Starting Message Send Test =="
write_start_time=$(date +%s.%N)

for room in "${rooms[@]}"; do
  room_start_time=$(date +%s.%N)

  for i in $(seq 1 $message_count); do
    msg="[$i in $room] $msg_content"

    curl --request POST \
      --url "$send_message_url" \
      --header 'Content-Type: application/json' \
      --data '{
        "user": "'"$user"'",
        "room": "'"$room"'",
        "msg": "'"${msg//\"/\\\"}"'"
      }' \
      --silent --write-out "Message $i to $room - HTTP %{http_code}\n" --output /dev/null

    total_messages=$((total_messages + 1))

    # Progress indicator every 10 messages
    if [ $((i % 10)) -eq 0 ]; then
      echo -n "."
    fi
  done
done

write_end_time=$(date +%s.%N)
total_write_duration=$(echo "$write_end_time - $write_start_time" | bc)
total_data_mb=$(echo "scale=2; $total_messages * $msg_size_kb / 1024" | bc)
overall_wps=$(echo "scale=2; $total_messages / $total_write_duration" | bc)
data_throughput_mbps=$(echo "scale=2; $total_data_mb / $total_write_duration" | bc)

echo
echo "==============================================="
echo "WRITE TEST RESULTS"
echo "==============================================="
echo "Total messages sent: $total_messages"
echo "Total data written: ${total_data_mb} MB"
echo "Total duration: ${total_write_duration}s"
echo "Average writes per second: ${overall_wps}"
echo "Data throughput: ${data_throughput_mbps} MB/s"
echo "Average message size: ${msg_size_kb} KB"
echo "Rooms processed: ${#rooms[@]}"
echo "==============================================="