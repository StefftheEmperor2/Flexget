from loguru import logger
from flexget.entry import Entry
from flexget.event import event
from flexget import plugin
from flexget.task import PluginWarning
import json
import itertools
from socket import gaierror
logger = logger.bind(name='beanstalkd')


class PluginBeanstalkdBase:
    schema = {
        'anyOf': [
            {'type': 'boolean'},
            {
                'type': 'object',
                'properties': {
                    'host': {'type': 'string'},
                    'port': {'type': 'integer'},
                    'tube': {'type': 'string'},
                    'chunk_size': {'type': 'integer', 'default': 30},
                    'timeout': {'type': 'integer', 'default': 1},
                    'delay': {'type': 'integer', 'default': 3600}
                },
                'required': ['host', 'port', 'tube'],
                'additionalProperties': False,
            },
        ]
    }

    def get_queue(self, beanstalkd_module, config):
        try:
            queue = beanstalkd_module.Client(host=config.get('host'), port=config.get('port'), encoding='UTF-8')
        except (ConnectionRefusedError, gaierror):
            raise PluginWarning('Connection to beanstalkd failed')
        queue.use(config.get('tube'))
        return queue

    @staticmethod
    def get_beanstalkd_module():
        try:
            import greenstalk
        except:
            raise plugin.PluginError('greenstalk module required.', logger)
        return greenstalk


class PluginBeanstalkdInput(PluginBeanstalkdBase):
    def on_task_input(self, task, config):
        beanstalkd_module = self.get_beanstalkd_module()
        queue = self.get_queue(beanstalkd_module, config)
        queue.watch(config.get('tube'))
        beanstalkd_entries = []
        try:
            beanstalkd_entries.append(queue.reserve(config.get('timeout')))
        except beanstalkd_module.TimedOutError:
            pass

        do_continue = True
        entry_count = 0
        entries = []
        while beanstalkd_entries and do_continue:
            for beanstalkd_entry in beanstalkd_entries:
                entry_data = beanstalkd_entry.body
                try:
                    entry = Entry.deserialize(json.loads(entry_data), None)
                    entry['beanstalkd_id'] = beanstalkd_entry.id
                    entries.append(entry)
                except json.decoder.JSONDecodeError:
                    logger.error('Could not decode job {beanstalkd_entry.id}')
                    pass
                try:
                    beanstalkd_entries.remove(beanstalkd_entry)
                except beanstalkd_module.NotFoundError:
                    logger.error('Job {beanstalkd_entry.id} has gone away')
                entry_count += 1
            if entry_count > config['chunk_size']:
                do_continue = False
            else:
                try:
                    beanstalkd_entries.append(queue.reserve(config.get('timeout')))
                except beanstalkd_module.TimedOutError:
                    pass
        queue.close()
        return entries

    def on_task_output(self, task, config):
        beanstalkd_module = self.get_beanstalkd_module()
        queue = self.get_queue(beanstalkd_module, config)

        if task.accepted and task.rejected:
            entries_to_delete = itertools.chain(task.accepted, task.rejected)
        elif task.accepted:
            entries_to_delete = task.accepted
        elif task.rejected:
            entries_to_delete = task.rejected
        else:
            entries_to_delete = []

        for entry in entries_to_delete:
            if 'beanstalkd_id' in entry:
                beanstalkd_id = entry['beanstalkd_id']
                try:
                    queue.peek(beanstalkd_id)
                    queue.delete(beanstalkd_id)
                except beanstalkd_module.NotFoundError:
                    logger.error('Job {beanstalkd_id} has gone away')

        if task.undecided:
            for entry in task.undecided:
                if 'beanstalkd_id' in entry:
                    beanstalkd_id = entry['beanstalkd_id']
                    queue.peek(beanstalkd_id)
                    queue.release(beanstalkd_id, delay=config.get('delay'))
        queue.close()


class PluginBeanstalkdOutput(PluginBeanstalkdBase):
    def on_task_output(self, task, config):
        if not task.accepted:
            return

        beanstalkd_module = self.get_beanstalkd_module()
        queue = self.get_queue(beanstalkd_module, config)
        for entry in task.accepted:
            try:
                queue.put(json.dumps(Entry.serialize(entry)).encode('UTF-8'))
            except Exception as e:
                entry.fail(e)
        queue.close()


@event('plugin.register')
def register_plugin():
    plugin.register(PluginBeanstalkdInput, 'from_beanstalkd', api_ver=2)
    plugin.register(PluginBeanstalkdOutput, 'beanstalkd', api_ver=2)