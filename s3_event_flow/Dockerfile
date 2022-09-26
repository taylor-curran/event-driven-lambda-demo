FROM public.ecr.aws/lambda/python:3.9
RUN pip install prefect prefect-aws s3fs
ADD s3_event_flow.py .
CMD ["s3_event_flow.handler"]