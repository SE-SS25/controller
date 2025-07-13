#!/bin/bash

# Configuration
rooms=("alpha" "bravo" "golf" "hotel" "mike" "november" "sierra" "tango")
messages_per_request=10    # Number of messages to retrieve per request (n parameter)
requests_per_room=20       # Number of requests to make per room
base_url="http://localhost:8080/v1/post"

# Metrics variables
total_requests=100
total_messages_retrieved=10
total_data_kb=total_messages_retrieved * 32  # Rough estimate of data size in KB
read_start_time=""
read_end_time=""

echo "==============================================="
echo "READ TEST CONFIGURATION"
echo "==============================================="
echo "Rooms: ${#rooms[@]}"
echo "Messages per request: $messages_per_request"
echo "Requests per room: $requests_per_room"
echo "Total requests to make: $((${#rooms[@]} * requests_per_room))"
echo "Expected total messages: $((${#rooms[@]} * requests_per_room * messages_per_request))"
echo "Base URL: $base_url"
echo "==============================================="
echo

echo "== Starting Message Read Test =="
read_start_time=$(date +%s.%N)

for room in "${rooms[@]}"; do
  echo "Reading messages from room: $room"
  room_start_time=$(date +%s.%N)
  room_requests=0
  room_messages=0
  room_data_kb=0

  for i in $(seq 1 $requests_per_room); do
    # Make the request and capture both response and timing
    response=$(curl --request GET \
      --url "${base_url}/${room}?n=${messages_per_request}" \
      --silent \
      --write-out "HTTPCODE:%{http_code};SIZE:%{size_download}")

    # Extract HTTP code and response size
    http_code=$(echo "$response" | grep -o "HTTPCODE:[0-9]*" | cut -d: -f2)
    response_size=$(echo "$response" | grep -o "SIZE:[0-9]*" | cut -d: -f2)

    # Remove the metrics from the actual response
    actual_response=$(echo "$response" | sed 's/HTTPCODE:[0-9]*;SIZE:[0-9]*$//')

    room_requests=$((room_requests + 1))
    total_requests=$((total_requests + 1))

    if [ "$http_code" = "200" ]; then
      # Count messages in response (assuming JSON array format)
      # This is a rough estimate - adjust based on your actual response format
      message_count_in_response=$(echo "$actual_response" | grep -o '{"' | wc -l)

      room_messages=$((room_messages + message_count_in_response))
      total_messages_retrieved=$((total_messages_retrieved + message_count_in_response))

      # Convert response size from bytes to KB
      response_size_kb=$(echo "scale=2; $response_size / 1024" | bc)
      room_data_kb=$(echo "$room_data_kb + $response_size_kb" | bc)
      total_data_kb=$(echo "$total_data_kb + $response_size_kb" | bc)
    fi

    echo "Request $i to $room - HTTP $http_code (${response_size} bytes)"

    # Progress indicator every 5 requests
    if [ $((i % 5)) -eq 0 ]; then
      echo -n "."
    fi

    # Small delay to avoid overwhelming the server
    sleep 0.1
  done

  room_end_time=$(date +%s.%N)
  room_duration=$(echo "$room_end_time - $room_start_time" | bc)
  room_rps=$(echo "scale=2; $room_requests / $room_duration" | bc)
  room_mps=$(echo "scale=2; $room_messages / $room_duration" | bc)

  echo
  echo "Room $room completed: $room_requests requests, $room_messages messages, $(echo "scale=2; $room_data_kb" | bc) KB in ${room_duration}s"
  echo "Room $room rates: ${room_rps} requests/sec, ${room_mps} messages/sec"
  echo "--------------------------"
done

read_end_time=$(date +%s.%N)
total_read_duration=$(echo "$read_end_time - $read_start_time" | bc)
overall_rps=$(echo "scale=2; $total_requests / $total_read_duration" | bc)
overall_mps=$(echo "scale=2; $total_messages_retrieved / $total_read_duration" | bc)
data_throughput_kbps=$(echo "scale=2; $total_data_kb / $total_read_duration" | bc)
data_throughput_mbps=$(echo "scale=2; $data_throughput_kbps / 1024" | bc)
avg_messages_per_request=$(echo "scale=2; $total_messages_retrieved / $total_requests" | bc)

echo
echo "==============================================="
echo "READ TEST RESULTS"
echo "==============================================="
echo "Total requests made: $total_requests"
echo "Total messages retrieved: $total_messages_retrieved"
echo "Average messages per request: $avg_messages_per_request"
echo "Total data read: $(echo "scale=2; $total_data_kb / 1024" | bc) MB"
echo "Total duration: ${total_read_duration}s"
echo "Average requests per second: ${overall_rps}"
echo "Average messages per second: ${overall_mps}"
echo "Data throughput: ${data_throughput_mbps} MB/s"
echo "Rooms processed: ${#rooms[@]}"
echo "==============================================="