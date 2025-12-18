build *args:
  cargo xtask build {{args}}

service *args:
  cargo xtask service {{args}}

precheckin *args:
  cargo xtask precheckin {{args}}

run-tests *args:
  cargo xtask run-tests {{args}}

deploy *args:
  cargo xtask deploy {{args}}

describe-stack *args:
  cargo xtask tools describe-stack {{args}}

dump-vg-config *args:
  cargo xtask tools dump-vg-config {{args}}

repo *args:
  cargo xtask repo {{args}}

git *args:
  cargo xtask repo foreach git {{args}}
