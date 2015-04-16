require 'thread'

module Celluloid
  class MailboxDead < Celluloid::Error; end # you can't receive from the dead
  class MailboxShutdown < Celluloid::Error; end # raised if the mailbox can no longer be used

  # Actors communicate with asynchronous messages. Messages are buffered in
  # Mailboxes until Actors can act upon them.
  class Mailbox
    include Enumerable

    # A unique address at which this mailbox can be found
    attr_reader :address
    attr_accessor :max_size

    def initialize
      @address   = Celluloid.uuid

      # TODO: use a Queue + Timeout.timeout{} instead?
      @messages  = []

      @mutex     = Mutex.new
      @dead      = false
      @condition = ConditionVariable.new
      @max_size  = nil
    end

    # Add a message to the Mailbox
    def <<(message)
      @mutex.lock
      begin
        if mailbox_full || @dead
          # Signal anyway - in case the condmutex is waiting
          # (especially if it's full, we want messages to get processed)
          @condition.signal

          dead_letter(message)
          return
        end

        if message.is_a?(SystemEvent)
          # SystemEvents are high priority messages so they get added to the
          # head of our message queue instead of the end
          @messages.unshift message
        else
          fail "NIL messages not allowed!" if message.nil?
          @messages << message
        end

        @condition.signal
        nil
      ensure
        @mutex.unlock rescue nil
      end
    end

    # Receive a message from the Mailbox. May return nil and may return before
    # the specified timeout.
    def check(timeout = nil, &block)
      @mutex.lock
      begin
        raise MailboxDead, "attempted to receive from a dead mailbox" if @dead
        wait_for_message(timeout, &block)
      ensure
        @mutex.unlock rescue nil
      end
    end

    # Receive a letter from the mailbox. Guaranteed to return a message. If
    # timeout is exceeded, raise a TimeoutError.
    def receive(timeout = nil, &block)
      Timers::Wait.for(timeout) do |remaining|
        if message = check(remaining, &block)
          return message
        end
      end

      raise TimeoutError.new("receive timeout exceeded")
    end

    # Shut down this mailbox and clean up its contents
    def shutdown
      messages = []
      @mutex.lock
      raise MailboxDead, "mailbox already shutdown" if @dead
      begin
        yield if block_given?
        messages = @messages
        @messages = []
        @dead = true

        # let the condmutex timeout if we acquired this lock during it's wait()
        @condition.signal
      ensure
        @mutex.unlock rescue nil
      end

      messages.each do |msg|
        dead_letter msg
        msg.cleanup if msg.respond_to? :cleanup
      end
      true
    end

    # Is the mailbox alive?
    def alive?
      @mutex.synchronize { !@dead }
    end

    # Cast to an array
    def to_a
      @mutex.synchronize { @messages.dup }
    end

    # Iterate through the mailbox
    def each(&block)
      to_a.each(&block)
    end

    # Inspect the contents of the Mailbox
    def inspect
      "#<#{self.class}:#{object_id.to_s(16)} @messages=[#{map(&:inspect).join(', ')}]>"
    end

    # Number of messages in the Mailbox
    def size
      @mutex.synchronize { @messages.size }
    end

    private

    # Retrieve the next message in the mailbox
    def next_message
      message = nil

      if block_given?
        index = @messages.index do |msg|
          yield(msg) || msg.is_a?(SystemEvent)
        end

        message = @messages.slice!(index, 1).first if index
      else
        message = @messages.shift
      end

      message
    end

    def dead_letter(message)
      Logger.debug "Discarded message (mailbox is dead): #{message}" if $CELLULOID_DEBUG
    end

    def mailbox_full
      @max_size && @messages.size >= @max_size
    end

    def wait_for_message(timeout, &block)
      Timers::Wait.for(timeout) do |remaining|
        # TODO: there should be a "peek" method
        message = next_message(&block)
        return message if message

        # Try and wait for next message
        @condition.wait(@mutex, remaining)
        return nil if @dead

        # Avoid timing out if there are message present
        # (we don't know if the
        message = next_message(&block)
        return message if message
      end
    end
  end
end