data "terraform_remote_state" "vpc" {
  backend = "s3"
  config = {
    bucket = "mybucket"
    key    = "path/to/my/key"
    region = "us-west-2"
  }
}

resource "aws_docdb_cluster" "default" {
  cluster_identifier      = "my-docdb-cluster"
  engine                  = "docdb"
  master_username         = "foo"
  master_password         = "bar"
  backup_retention_period = 5
  preferred_backup_window = "07:00-09:00"
  skip_final_snapshot     = true
}

resource "aws_docdb_subnet_group" "default" {
  name       = "main"
  subnet_ids = data.terraform_remote_state.vpc.outputs.subnet_ids
}

resource "aws_docdb_cluster_instance" "cluster_instances" {
  count              = 2
  identifier         = "docdb-cluster-instance-${count.index}"
  cluster_identifier = aws_docdb_cluster.default.id
  instance_class     = "db.r5.large"
}

resource "aws_security_group" "docdb" {
  name   = "docdb"
  vpc_id = data.terraform_remote_state.vpc.outputs.vpc_id

  ingress {
    from_port   = 27017
    to_port     = 27017
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}