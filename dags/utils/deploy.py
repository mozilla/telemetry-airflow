
def get_artifact_url(slug, region="{{ task.__class__.region }}",
                     bucket="{{ task.__class__.artifacts_bucket }}",
                     owner_slug="{{ test.__class__.mozilla_slug }}",
                     deploy_tag="{{ task.__class__.deploy_tag }}"):
    return "https://s3-{region}.amazonaws.com/{bucket}/{owner_slug}/{slug}/{deploy_tag}/{slug}.jar".format(
        region=region, bucket=bucket, owner_slug=owner_slug, slug=slug, deploy_tag=deploy_tag)

