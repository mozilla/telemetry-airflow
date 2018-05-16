
def get_artifact_url(slug, branch=None, tag=None,
                     region="{{ task.__class__.region }}",
                     bucket="{{ task.__class__.artifacts_bucket }}",
                     owner_slug="{{ task.__class__.mozilla_slug }}",
                     deploy_tag="{{ task.__class__.deploy_tag }}"):
    assert not (branch and tag), "Cannot build from both a branch and a tagged release"

    if branch:
        deploy_tag = branch
    elif tag:
        deploy_tag = "tags/{}".format(tag)

    return "https://s3-{region}.amazonaws.com/{bucket}/{owner_slug}/{slug}/{deploy_tag}/{slug}.jar".format(
        region=region, bucket=bucket, owner_slug=owner_slug, slug=slug, deploy_tag=deploy_tag)

