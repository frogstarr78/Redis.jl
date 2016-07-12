NAME = evil_bartik
CMD = /bin/bash
.PHONY: tests
tests:
	julia -e 'Pkg.test("Redis")'

commands_by_group: commands.json
	grep ': {\|group' commands.json | tr -s ' ' | tr 'A-Z' 'a-z' | sed -e 's/^ *//g' -e 's/"group": "/ /g' -e 's/": {//g' | tr -d "\n" | sed 's/"/\n/g' | strings | sort -k 2d

cp_to_container:
	docker cp . $(NAME):/root/.julia/v0.4/Redis/

attach:
	docker exec -it $(NAME) $(CMD)
