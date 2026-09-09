library(argparser)

parser <- arg_parser("Example script for cfa-cloudops")
parser <- add_argument(
  parser,
  "--user",
  default = "world",
  help = "User name to greet"
)

args <- parse_args(parser)

cat(sprintf("Hello, %s!\n", args$user))
