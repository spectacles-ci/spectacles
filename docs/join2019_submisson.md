#### Session Title

### Develop LookML Like a Software Engineer: Continuous Integration for Looker

#### Session Abstract

Occasionally, when we make changes to LookML or transform an underlying database table, we break downstream experiences in Looker by introducing database or content errors. These bugs are easy to deploy to production unknowingly, since they are runtime errors, not compiler errors. In short, they aren't discovered until a user runs a query. For little-used fields, these bugs can remain hidden for weeks or months.

We wanted a tool that would check for fields with database errors and content with LookML errors. We firmly believe that analytics should adopt best practices from software engineering, and we're especially excited by the power of continuous integration (CI). We could run this tool in a CI pipeline before deploying to production or after database changes, helping to establish a baseline performance expectation for our Looker instance.

We built a new tool, `spectacles`, to enhance the business intelligence layer of analytics CI pipelines.

`spectacles` is built on the Looker API and automatically runs queries across every enabled explore to check for database errors. Next, it performs content validation to catch any content that newly introduced code may have broken. Finally, `spectacles` can be run in conjunction with database code changes to catch cases where transformations will break a downstream Looker view. We believe `spectacles` is a valuable addition to the "Built on Looker" open-source ecosystem and will improve quality and reliability within Looker.

#### Three primary concepts the audience will take away from your session.

1. Extending testing and validation into the BI layer
2. Continuous integration for Looker
2. Building on Looker using the Looker API

#### Please explain why this topic is a good fit for the chosen track. (Developing on Looker)

We will walk through a valuable tool we custom built using a variety of endpoints within the Looker API.

#### Would you be willing to combine your story/topic with other speakers on similar topics as part of a joint presentation?

Yes.

#### Do the proposed speakers have prior experience presenting at events of this type? Please include links to any public recordings, if available.

Yes. Josh and Dylan are both regular speakers at data-related events. Josh has spoken and taught at a number of events, notably those hosted by The Flatiron School, Dataiku, and New York University. Dylan has also spoken and taught at a number of events, notably those hosted by Looker and DataCouncil. Dylan is the organiser of the London dbt meetup.
