#!/usr/bin/ruby

require 'rubygems'
require 'resque'
require 'getoptlong'

def get_stats (items)
	resque_info = Resque.info
	result = []

	items.each do |key|
		result.push("#{key}:#{resque_info[@valid_items[key]]}")
	end

	print result.join(' ')
end

def get_index()
	for queue,index in Resque.queues.each_with_index
		printf("%s\n", queue)
	end
end

def get_num_indexes()
	printf("%d\n", Resque.queues.size)
end

def get_dq_query(query)
	queues = Resque.queues

	for queue,index in Resque.queues.each_with_index
		if (query == 'qname')
			printf("%1$s:%1$s\n", queue)
		elsif (query == 'jobs')
			printf("%s:%d\n", queue, Resque.size(queue))
		end
	end
end

def get_dq_value(value, queue)
	queues = Resque.queues

	if (queues.include? queue)
		if (value == 'jobs')
			printf("%d\n", Resque.size(queue))
		else
			printf("ERROR: invalid --get value\n")
			exit(false)
		end
	else
		printf("ERROR: invalid --get queue name\n")
		exit(false)
	end
end

def usage
	puts <<-EOF

resque_stats.rb [OPTION]

--help, -h
   show help

--host <hostname>:<port>
   Redis-Server to connect to, e.g. 'redis://my.redis.com:6379'

Cacti Data Query
----------------

--mode dq
   Cacti Data Query

--index

--get

--num_indexes

Cacti Input Method
------------------

--mode im
   Cacti Input Method

--items [items]
   Comma-separated list of the items whose data you want

   Valid items are:

   + pj		Pending Jobs
   + fj		Failed Jobs
   + nq		Number of Queues
   + nw		Number of Workers
   + nww	Number of Workers working

	EOF
end

def main
	opts = GetoptLong.new(
		[ '--help', '-h', GetoptLong::NO_ARGUMENT ],
		[ '--host', GetoptLong::REQUIRED_ARGUMENT ],
		[ '--items', GetoptLong::REQUIRED_ARGUMENT ],
		[ '--mode', GetoptLong::REQUIRED_ARGUMENT ],
		[ '--index', GetoptLong::NO_ARGUMENT ],
		[ '--query', GetoptLong::REQUIRED_ARGUMENT ],
		[ '--num_indexes', GetoptLong::NO_ARGUMENT ],
		[ '--get', GetoptLong::REQUIRED_ARGUMENT ]
	)

	@valid_items = {
		'pj'  => :pending,
		'nq'  => :queues,
		'nw'  => :workers,
		'nww' => :working,
		'fj'  => :failed
	}

	if ARGV[0] != nil && ARGV.size > 0
		opts.each do |opt, arg|
			case opt
				when '--help'
					usage()
				when '--host'
					@host = arg.to_s
					Resque.redis = @host
				when '--mode'
					@mode = arg.to_s
					if (@mode != 'dq' && @mode != 'im')
						printf("\nERROR: invalid mode - supported: dq or im\n\n")
						exit(false)
					end
				when '--index'
					@dq_arg = 'index'
				when '--num_indexes'
					@dq_arg = 'num_indexes'
				when '--query'
					@dq_query = arg.to_s
				when '--get'
					@dq_get_value = arg.to_s
					@dq_get_queue = ARGV[-1]
				when '--items'
					@items = arg.split(',')

					invalid_items = @items - @valid_items.keys
					if (invalid_items.size == 1)
						printf("\nERROR: '%s' is not a valid item!\n\n", invalid_items[0])
						exit(false)
					elsif (invalid_items.size > 1)
						printf("\nERROR: '%s' are not valid items!\n\n", invalid_items.join('\' and \''))
						exit(false)
					end
			end
		end

		if (@host && @mode == 'dq')
			if (@dq_arg == 'index')
				get_index()
			elsif (@dq_arg == 'num_indexes')
				get_num_indexes()
			elsif (@dq_query)
				get_dq_query(@dq_query)
			elsif (@dq_get_value && @dq_get_queue)
				get_dq_value(@dq_get_value, @dq_get_queue)
			else
				usage()
			end
		elsif (@host && @mode == 'im')
			get_stats(@items)
		else
			usage()
		end
	else
		usage()
	end
end

if __FILE__==$0
	begin
		main
	rescue Interrupt => e
		nil
	end
end
