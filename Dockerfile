FROM rayproject/ray:2.55.0-py312

RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple \
    daft==0.7.15 pyarrow==19.0.1 s3fs==2026.4.0 numpy==1.26.4 boto3==1.43.46
