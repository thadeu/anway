require "faktory_worker_ruby"

mode, jid = ARGV

Faktory::Client.worker! unless mode == "push"
client = Faktory::Client.new(url: ENV.fetch("FAKTORY_URL"))

if mode == "beat"
  seen = nil

  loop do
    state = client.beat
    puts "ruby beat #{state}" if state != "OK" && state != seen
    seen = state
    break if state == "terminate"

    sleep 0.1
  end
elsif mode == "push"
  client.push("jid" => jid, "jobtype" => "compat", "args" => ["ruby", 42], "queue" => "default")
else
  job = client.fetch("default")
  raise "unexpected job: #{job.inspect}" unless job && job["jid"] == jid

  if mode == "fail"
    client.fail(jid, RuntimeError.new("compat failure"))
    client.close
    exit
  end

  client.ack(jid)
  puts "ruby consumed #{jid} with args #{job["args"]}"
end

client.close
