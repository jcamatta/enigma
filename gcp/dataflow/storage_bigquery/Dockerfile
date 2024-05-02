ARG project=
ARG location=
ARG repo=
ARG base_image=
ARG base_image_tag=

FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:20231225-rc00 as template_launcher
FROM ${location}-docker.pkg.dev/${project}/${repo}/${base_image}:${base_image_tag}

COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

ARG WORKDIR=/template/
WORKDIR ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/main.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py
ENV RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1

ENV PYTHONPATH=${WORKDIR}
ENV PYTHONPATH=${WORKDIR}/direct_gcs_bq:${PYTHONPATH}

COPY main.py setup.py ${WORKDIR}/

RUN mkdir -p ${WORKDIR}/direct_gcs_bq
COPY ./direct_gcs_bq/* ${WORKDIR}/direct_gcs_bq/

RUN python3 -m pip install --upgrade pip \
    && python3 -m pip install --upgrade setuptools ${WORKDIR}/.

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]