terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "region"         { default = "us-east-1" }
variable "cluster_name"   { default = "infra-ops" }
variable "cluster_version"{ default = "1.31" }   # check your preferred EKS version
variable "node_instance"  { default = "t3.medium" }
variable "desired_size"   { default = 2 }
variable "min_size"       { default = 1 }
variable "max_size"       { default = 3 }

provider "aws" {
  region = var.region
}

# ---------- Network (VPC + subnets) ----------
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = "${var.cluster_name}-vpc"
  cidr = "10.0.0.0/16"

  azs             = ["${var.region}a", "${var.region}b"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24"]

  enable_nat_gateway = true
  single_nat_gateway = true

  tags = {
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.8"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version

  cluster_endpoint_public_access = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    default = {
      min_size     = var.min_size
      max_size     = var.max_size
      desired_size = var.desired_size

      instance_types = [var.node_instance]
      capacity_type  = "ON_DEMAND"
    }
  }

  tags = {
    Project = var.cluster_name
  }
}

output "cluster_name"     { value = module.eks.cluster_name }
output "cluster_endpoint" { value = module.eks.cluster_endpoint }
output "cluster_version"  { value = module.eks.cluster_version }
output "node_group_role_arn" {
  value = try(module.eks.eks_managed_node_groups["default"].iam_role_arn, null)
}
