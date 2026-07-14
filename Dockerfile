FROM rayproject/ray:2.55.0-py312

RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple \
    daft==0.7.15 pyarrow s3fs numpy && \
    pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -U boto3
