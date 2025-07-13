#!/bin/bash

# NATO Phonetic Alphabet (A–Z)
rooms=(
  "alpha" "bravo" "charlie" "delta" "echo" "foxtrot" "golf" "hotel" "india"
  "juliett" "kilo" "lima" "mike" "november" "oscar" "papa" "quebec" "romeo"
  "sierra" "tango" "uniform" "victor" "whiskey" "xray" "yankee" "zulu"
)

# User allowed in each room
user="Leon"

# API endpoint
url="http://localhost:80/v1/addroom"

echo "== Creating NATO Alphabet Rooms =="

for room in "${rooms[@]}"; do
  echo "Creating room: $room"

  curl --request POST \
    --url "$url" \
    --header 'Content-Type: application/json' \
    --data '{
      "name": "'"$room"'",
      "allowed_users": ["'"$user"'"]
    }' \
    --silent --write-out "Room $room - HTTP %{http_code}\n" --output /dev/null

  sleep 0.2
done

echo "✅ All NATO alphabet rooms submitted."