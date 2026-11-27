FROM python:3.11-slim
WORKDIR /app
# dependances installees depuis des wheels telechargees par ADD (aucun acces reseau requis dans le conteneur)
ADD https://files.pythonhosted.org/packages/3a/d0/edc009c27b406c4f9cbc79274d6e46d634d139075492ad055e3d68445925/numpy-1.26.4-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl /tmp/wheels/
ADD https://files.pythonhosted.org/packages/fc/a5/4d82be566f069d7a9a702dcdf6f9106df0e0b042e738043c0cc7ddd7e3f6/pandas-2.2.2-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl /tmp/wheels/
ADD https://files.pythonhosted.org/packages/d2/34/4e3c04e7398764e56ef00f8f267f8ebf565808478f5fee850cef4be670c3/pyarrow-16.1.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl /tmp/wheels/
ADD https://files.pythonhosted.org/packages/ec/57/56b9bcc3c9c6a792fcbaf139543cee77261f3651ca9da0c93f5c1221264b/python_dateutil-2.9.0.post0-py2.py3-none-any.whl /tmp/wheels/
ADD https://files.pythonhosted.org/packages/9c/3d/a121f284241f08268b21359bd425f7d4825cffc5ac5cd0e1b3d82ffd2b10/pytz-2024.1-py2.py3-none-any.whl /tmp/wheels/
ADD https://files.pythonhosted.org/packages/65/58/f9c9e6be752e9fcb8b6a0ee9fb87e6e7a1f6bcab2cdc73f02bb7ba91ada0/tzdata-2024.1-py2.py3-none-any.whl /tmp/wheels/
ADD https://files.pythonhosted.org/packages/d9/5a/e7c31adbe875f2abbb91bd84cf2dc52d792b5a01506781dbcf25c91daf11/six-1.16.0-py2.py3-none-any.whl /tmp/wheels/
ADD https://files.pythonhosted.org/packages/75/68/dcb0db055309f680ab2931a3eeb22d865604b638acf8c914bedf4c1a0c8c/kafka_python-2.0.2-py2.py3-none-any.whl /tmp/wheels/
RUN pip install --no-index --find-links /tmp/wheels numpy==1.26.4 pandas==2.2.2 pyarrow==16.1.0 kafka-python==2.0.2 && rm -rf /tmp/wheels
COPY . .
