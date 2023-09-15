from string import Template

# TODO do not use template here, but for now before finding best approach

CREATE_ALIAS = Template("""CREATE ALIAS IF NOT EXISTS $alias FOR \"$func\";""")
