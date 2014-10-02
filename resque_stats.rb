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

def usage
	puts <<-EOF

resque_stats [OPTION]

--help
   show help

--host <hostname>:<port>, -h <hostname>:<port>
   Redis-Server to connect to, e.g. 'redis://my.redis.com:6379'

--items [items]
   Comma-separated list of the items whose data you want

   Valid items are:

   + pj		Pending Jobs
   + nq		Number of Queues
   + nw		Number of Workers
   + nww	Number of Workers working
   + nf		Number of failed Jobs

	EOF
end

def main
	opts = GetoptLong.new(
		[ '--help', GetoptLong::NO_ARGUMENT ],
		[ '--host', '-h', GetoptLong::REQUIRED_ARGUMENT ],
		[ '--items', '-i', GetoptLong::REQUIRED_ARGUMENT ]
	)

	@valid_items = {
		'pj'  => :pending,
		'nq'  => :queues,
		'nw'  => :workers,
		'nww' => :working,
		'nf'  => :failed
	}

	if ARGV[0] != nil && ARGV.size > 0
		opts.each do |opt, arg|
			case opt
				when '--help'
					usage()
				when '--host'
					@host = arg.to_s
					Resque.redis = @host
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

		if @host && @items
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
